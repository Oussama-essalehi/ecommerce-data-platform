"""Publishes the web shop's order events, history first, then live.

The producer keeps a cursor: the instant up to which events were published.
It is saved to a checkpoint file after each flushed batch, so a restart
continues from there. Delivery is therefore at-least-once: a crash between
the flush and the checkpoint re-sends one batch, and consumers are expected
to deduplicate on `event_id`.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, time as dtime, timedelta
from pathlib import Path
from typing import Callable

from .events import EventStream
from .settings import UTC, Settings
from .sinks import Sink
from .world import iso

log = logging.getLogger(__name__)

REPLAY_WINDOW = timedelta(days=1)


def history_start(settings: Settings) -> datetime:
    # One day of margin: the first Paris business day starts before UTC midnight.
    return datetime.combine(settings.start_date, dtime.min, tzinfo=UTC) - timedelta(days=1)


class Checkpoint:
    def __init__(self, path: Path):
        self.path = path

    def load(self) -> datetime | None:
        if not self.path.exists():
            return None
        data = json.loads(self.path.read_text(encoding="utf-8"))
        return datetime.fromisoformat(data["cursor"].replace("Z", "+00:00"))

    def save(self, cursor: datetime) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.path.with_suffix(".tmp")
        tmp_path.write_text(json.dumps({"cursor": iso(cursor)}), encoding="utf-8")
        os.replace(tmp_path, self.path)

    def clear(self) -> None:
        self.path.unlink(missing_ok=True)


def run(
    stream: EventStream,
    sink: Sink,
    checkpoint: Checkpoint,
    *,
    follow: bool = True,
    backfill: bool = True,
    tick_seconds: float = 5.0,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    should_stop: Callable[[], bool] = lambda: False,
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    """Publish events from the cursor up to now, then keep following if asked.

    Returns the number of messages sent.
    """
    cursor = checkpoint.load()
    if cursor is not None:
        log.info("resuming from checkpoint %s", iso(cursor))
    elif backfill:
        cursor = history_start(stream.settings)
        log.info("no checkpoint: replaying history from %s", stream.settings.start_date)
    else:
        cursor = clock()
        log.info("no checkpoint: starting from now")

    sent = 0
    while not should_stop():
        now = clock()
        # Catch up in bounded windows so a long replay checkpoints regularly.
        while cursor < now and not should_stop():
            window_end = min(cursor + REPLAY_WINDOW, now)
            messages = stream.between(cursor, window_end)
            for message in messages:
                sink.send(message)
            sink.flush()
            checkpoint.save(window_end)
            sent += len(messages)
            if window_end - cursor >= REPLAY_WINDOW:
                log.info("replayed %s: %d messages (%d total)",
                         cursor.date(), len(messages), sent)
            elif messages:
                log.info("published %d messages up to %s", len(messages), iso(window_end))
            cursor = window_end
        if not follow:
            break
        sleep(tick_seconds)
    log.info("stopped at %s after %d messages", iso(cursor), sent)
    return sent
