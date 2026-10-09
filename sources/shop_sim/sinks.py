"""Where the producer writes messages: Kafka, or a file/stdout for local runs."""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Protocol

from .events import Message

log = logging.getLogger(__name__)


class Sink(Protocol):
    def send(self, message: Message) -> None: ...
    def flush(self) -> None: ...
    def close(self) -> None: ...


class MemorySink:
    """Keeps messages in a list. Used by the tests."""

    def __init__(self) -> None:
        self.messages: list[Message] = []

    def send(self, message: Message) -> None:
        self.messages.append(message)

    def flush(self) -> None:
        pass

    def close(self) -> None:
        pass


class LineSink:
    """One message value per line, to stdout or to a file."""

    def __init__(self, path: Path | None = None):
        if path is None:
            self._handle = sys.stdout
            self._owned = False
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            self._handle = open(path, "a", encoding="utf-8")
            self._owned = True

    def send(self, message: Message) -> None:
        self._handle.write(message.value.decode("utf-8") + "\n")

    def flush(self) -> None:
        self._handle.flush()

    def close(self) -> None:
        self.flush()
        if self._owned:
            self._handle.close()


class KafkaSink:
    """Publishes to a Kafka topic with an idempotent producer.

    The Kafka record timestamp is left to the broker (wall-clock time): the
    business time of an event lives in its payload (`event_ts`). Stamping
    records with historical times during a replay would make them eligible
    for time-based retention as soon as they are written.
    """

    def __init__(self, bootstrap_servers: str, topic: str):
        from confluent_kafka import Producer  # imported here so tests do not need a broker

        self.topic = topic
        self.errors = 0
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": "shop-sim-producer",
                "enable.idempotence": True,
                "acks": "all",
                "linger.ms": 50,
                "compression.type": "lz4",
            }
        )

    def _on_delivery(self, error, _message) -> None:
        if error is not None:
            self.errors += 1
            log.error("delivery failed: %s", error)

    def send(self, message: Message) -> None:
        while True:
            try:
                self._producer.produce(
                    self.topic,
                    key=message.key.encode("utf-8"),
                    value=message.value,
                    on_delivery=self._on_delivery,
                )
                break
            except BufferError:
                # Local queue is full: let the client drain it, then retry.
                self._producer.poll(0.5)
        self._producer.poll(0)

    def flush(self) -> None:
        remaining = self._producer.flush(30)
        if remaining:
            raise RuntimeError(f"{remaining} messages were not delivered to Kafka within 30s")
        if self.errors:
            raise RuntimeError(f"{self.errors} messages were rejected by Kafka")

    def close(self) -> None:
        self.flush()
