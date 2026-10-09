from __future__ import annotations

from datetime import datetime, timedelta

from shop_sim.events import EventStream
from shop_sim.producer import Checkpoint, history_start, run
from shop_sim.settings import UTC
from shop_sim.sinks import LineSink, MemorySink


class Clock:
    """A clock the test moves by hand."""

    def __init__(self, now: datetime):
        self.now = now

    def __call__(self) -> datetime:
        return self.now


def test_replay_publishes_all_history_then_stops(book, settings, tmp_path):
    stream = EventStream(book)
    now = datetime(2025, 10, 11, 9, 30, tzinfo=UTC)
    sink, checkpoint = MemorySink(), Checkpoint(tmp_path / "cp.json")

    sent = run(stream, sink, checkpoint, follow=False, clock=Clock(now))

    assert sent == len(sink.messages) > 0
    assert sink.messages == stream.between(history_start(settings), now)
    assert checkpoint.load() == now


def test_restart_resumes_from_the_checkpoint_without_resending(book, tmp_path):
    stream = EventStream(book)
    clock = Clock(datetime(2025, 10, 5, tzinfo=UTC))
    checkpoint = Checkpoint(tmp_path / "cp.json")

    first = MemorySink()
    run(stream, first, checkpoint, follow=False, clock=clock)

    clock.now += timedelta(hours=6)
    second = MemorySink()
    run(stream, second, checkpoint, follow=False, clock=clock)

    assert second.messages == stream.between(clock.now - timedelta(hours=6), clock.now)
    assert len(second.messages) > 0
    last_of_first_run = max(m.emit_ts for m in first.messages)
    assert all(m.emit_ts > last_of_first_run for m in second.messages)


def test_live_mode_follows_the_clock_until_asked_to_stop(book, tmp_path):
    stream = EventStream(book)
    clock = Clock(datetime(2025, 10, 20, 18, 0, tzinfo=UTC))
    start = clock.now
    sink, checkpoint = MemorySink(), Checkpoint(tmp_path / "cp.json")
    ticks = []

    def sleep(seconds: float) -> None:
        ticks.append(seconds)
        clock.now += timedelta(minutes=10)

    run(
        stream, sink, checkpoint,
        follow=True, backfill=False, tick_seconds=5,
        clock=clock, sleep=sleep, should_stop=lambda: len(ticks) >= 6,
    )

    assert ticks == [5] * 6
    assert sink.messages == stream.between(start, start + timedelta(minutes=50))
    assert len(sink.messages) > 0


def test_without_backfill_history_is_skipped(book, tmp_path):
    sink = MemorySink()
    now = datetime(2025, 12, 1, tzinfo=UTC)
    run(EventStream(book), sink, Checkpoint(tmp_path / "cp.json"),
        follow=False, backfill=False, clock=Clock(now))
    assert sink.messages == []


def test_line_sink_writes_one_json_document_per_line(book, tmp_path):
    stream = EventStream(book)
    start = datetime(2025, 10, 2, tzinfo=UTC)
    messages = stream.between(start, start + timedelta(hours=12))
    path = tmp_path / "out" / "events.jsonl"
    sink = LineSink(path)
    for message in messages:
        sink.send(message)
    sink.close()
    lines = path.read_text(encoding="utf-8").splitlines()
    assert lines == [m.value.decode("utf-8") for m in messages]
