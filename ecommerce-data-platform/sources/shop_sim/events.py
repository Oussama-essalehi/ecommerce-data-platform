"""Order events of the web shop, as they are published to Kafka.

Two event types share one topic, keyed by order_id so that the events of an
order stay in the same partition:

  order_created         full order with its items
  order_status_changed  paid / shipped / delivered / cancelled / returned

`event_ts` is when the event happened. `emit_ts` is when it reaches the
broker; they differ for late events.
"""

from __future__ import annotations

import json
import uuid
from collections import OrderedDict
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta

from .orders import MAX_LIFECYCLE_DAYS, Order, OrderBook
from .settings import PARIS, Settings, rng_for
from .world import iso

SCHEMA_VERSION = 1
_NAMESPACE = uuid.UUID("6f1d3a52-0c57-4b8e-9a41-2f6f1f0a7c11")

# Injected stream defects, as probabilities per event (scaled by defect_rate).
P_LATE = 0.02            # delivered 1 to 30 minutes after it happened
P_DUPLICATE = 0.01       # delivered twice (at-least-once delivery)
P_NULL_CUSTOMER = 0.003  # order_created without a customer_id
P_TRUNCATED = 0.0005     # payload cut in half: not valid JSON


@dataclass(frozen=True, slots=True)
class Message:
    key: str
    value: bytes
    event_id: str
    event_ts: datetime
    emit_ts: datetime


def event_id_for(order_id: str, status: str) -> str:
    """Deterministic id: replaying the simulator yields the same event ids."""
    return str(uuid.uuid5(_NAMESPACE, f"{order_id}|{status}"))


def created_payload(order: Order) -> dict:
    return {
        "event_id": event_id_for(order.order_id, "created"),
        "event_type": "order_created",
        "event_ts": iso(order.created_at),
        "schema_version": SCHEMA_VERSION,
        "order_id": order.order_id,
        "customer_id": order.customer_id,
        "channel": order.channel,
        "payment_method": order.payment_method,
        "currency": "EUR",
        "shipping": {
            "city": order.shipping_city,
            "postal_code": order.shipping_postal_code,
            "fee": order.shipping_fee,
        },
        "items": [
            {
                "line_number": line.line_number,
                "product_id": line.product_id,
                "quantity": line.quantity,
                "unit_price": line.unit_price,
                "discount_pct": line.discount_pct,
            }
            for line in order.lines
        ],
        "total_amount": order.total_amount,
    }


def status_payload(order: Order, status: str, ts: datetime) -> dict:
    return {
        "event_id": event_id_for(order.order_id, status),
        "event_type": "order_status_changed",
        "event_ts": iso(ts),
        "schema_version": SCHEMA_VERSION,
        "order_id": order.order_id,
        "status": status,
    }


class EventStream:
    """Turns the web shop's orders into the messages a broker would receive."""

    def __init__(self, book: OrderBook, settings: Settings | None = None, cache_days: int = 64):
        self.book = book
        self.settings = settings or book.settings
        self._cache: OrderedDict[date, tuple[Message, ...]] = OrderedDict()
        self._cache_days = cache_days

    def between(self, start: datetime, end: datetime) -> list[Message]:
        """Messages delivered in [start, end), in delivery order.

        Consecutive windows never overlap and never leave gaps, so a producer
        can advance window by window and resume from where it stopped.
        """
        if end <= start:
            return []
        first_day = start.astimezone(PARIS).date() - timedelta(days=MAX_LIFECYCLE_DAYS + 1)
        last_day = end.astimezone(PARIS).date()
        messages: list[Message] = []
        day = max(first_day, self.settings.start_date)
        while day <= last_day:
            messages.extend(m for m in self._day_messages(day) if start <= m.emit_ts < end)
            day += timedelta(days=1)
        messages.sort(key=lambda m: (m.emit_ts, m.event_id))
        return messages

    def _day_messages(self, day: date) -> tuple[Message, ...]:
        """Every message caused by the orders created on `day`, whenever it is emitted."""
        if day in self._cache:
            self._cache.move_to_end(day)
            return self._cache[day]
        messages: list[Message] = []
        for order in self.book.day(day):
            if order.channel != "marketplace":  # marketplace orders arrive as CSV files
                messages.extend(self._order_messages(order))
        result = tuple(messages)
        self._cache[day] = result
        if len(self._cache) > self._cache_days:
            self._cache.popitem(last=False)
        return result

    def _order_messages(self, order: Order) -> list[Message]:
        rate = self.settings.defect_rate
        events = [(created_payload(order), order.created_at)]
        events += [(status_payload(order, c.status, c.ts), c.ts) for c in order.lifecycle]

        messages: list[Message] = []
        for payload, event_ts in events:
            rng = rng_for(self.settings.seed, "event", payload["event_id"])
            if payload["event_type"] == "order_created" and rng.random() < P_NULL_CUSTOMER * rate:
                payload["customer_id"] = None
            text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
            if rng.random() < P_TRUNCATED * rate:
                text = text[: len(text) // 2]
            emit_ts = event_ts
            if rng.random() < P_LATE * rate:
                emit_ts = event_ts + timedelta(seconds=rng.uniform(60, 1800))
            message = Message(
                key=order.order_id,
                value=text.encode("utf-8"),
                event_id=payload["event_id"],
                event_ts=event_ts,
                emit_ts=emit_ts,
            )
            messages.append(message)
            if rng.random() < P_DUPLICATE * rate:
                again = emit_ts + timedelta(seconds=rng.uniform(1, 120))
                messages.append(replace(message, emit_ts=again))
        return messages
