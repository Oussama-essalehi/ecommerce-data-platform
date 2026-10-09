from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timedelta

from shop_sim.events import EventStream
from shop_sim.settings import UTC

START = datetime(2026, 3, 10, 0, 0, tzinfo=UTC)


def _parse(messages):
    return [json.loads(m.value) for m in messages]


def test_windows_tile_the_timeline_without_gap_or_overlap(book):
    stream = EventStream(book)
    cuts = [START, START + timedelta(hours=7), START + timedelta(days=2, minutes=13),
            START + timedelta(days=5)]
    pieces = []
    for left, right in zip(cuts, cuts[1:]):
        pieces.extend(stream.between(left, right))
    whole = stream.between(cuts[0], cuts[-1])
    assert pieces == whole
    assert len(whole) > 500


def test_a_message_on_the_boundary_belongs_to_the_next_window(book):
    stream = EventStream(book)
    cut = stream.between(START, START + timedelta(days=1))[100].emit_ts
    before = stream.between(START, cut)
    after = stream.between(cut, START + timedelta(days=1))
    assert all(m.emit_ts < cut for m in before)
    assert after[0].emit_ts == cut
    assert before + after == stream.between(START, START + timedelta(days=1))


def test_messages_come_out_in_delivery_order_inside_the_window(book):
    messages = EventStream(book).between(START, START + timedelta(days=1))
    emitted = [m.emit_ts for m in messages]
    assert emitted == sorted(emitted)
    assert all(START <= ts < START + timedelta(days=1) for ts in emitted)


def test_only_web_shop_orders_are_streamed(clean_book):
    messages = EventStream(clean_book).between(START, START + timedelta(days=1))
    assert messages and all(m.key.startswith("WEB-") for m in messages)
    channels = {e["channel"] for e in _parse(messages) if e["event_type"] == "order_created"}
    assert channels == {"web", "mobile"}


def test_clean_stream_is_valid_ordered_and_unique(clean_book):
    messages = EventStream(clean_book).between(START, START + timedelta(days=3))
    events = _parse(messages)  # every payload is valid JSON
    assert len({e["event_id"] for e in events}) == len(events)
    assert all(m.emit_ts == m.event_ts for m in messages)
    assert all(m.key == e["order_id"] for m, e in zip(messages, events))
    for event in events:
        assert event["schema_version"] == 1
        assert event["event_ts"].endswith("Z")
        if event["event_type"] == "order_created":
            assert event["customer_id"]
            assert event["items"]
            lines = sum(
                round(i["quantity"] * i["unit_price"] * (1 - i["discount_pct"] / 100), 2)
                for i in event["items"]
            )
            assert event["total_amount"] == round(lines + event["shipping"]["fee"], 2)
        else:
            assert event["event_type"] == "order_status_changed"
            assert event["status"] in {"paid", "shipped", "delivered", "cancelled", "returned"}


def test_status_changes_of_orders_created_weeks_before_are_included(clean_book, clean_settings):
    stream = EventStream(clean_book)
    window_start = datetime(2026, 4, 1, tzinfo=UTC)
    events = _parse(stream.between(window_start, window_start + timedelta(days=1)))
    old = [e for e in events if e["order_id"] < "WEB-20260325"]
    assert old and all(e["event_type"] == "order_status_changed" for e in old)


def test_defects_are_injected_at_the_documented_rates(book, clean_book):
    span = (START, START + timedelta(days=30))
    dirty = EventStream(book).between(*span)
    clean = EventStream(clean_book).between(*span)

    duplicates = sum(count - 1 for count in Counter(m.event_id for m in dirty).values())
    late = sum(1 for m in dirty if m.emit_ts > m.event_ts)
    broken = null_customer = 0
    for message in dirty:
        try:
            event = json.loads(message.value)
        except json.JSONDecodeError:
            broken += 1
            continue
        if event["event_type"] == "order_created" and event["customer_id"] is None:
            null_customer += 1

    n = len(clean)
    assert n > 5_000
    assert 0.005 * n < duplicates < 0.02 * n
    assert 0.015 * n < late < 0.04 * n   # late events, plus the re-delivery of duplicates
    assert 0 < null_customer < 0.003 * n
    assert broken < 0.002 * n
    # Defects only add or alter messages: every clean event is still there.
    assert {m.event_id for m in clean} <= {m.event_id for m in dirty} | _edge(dirty, clean, span)


def _edge(dirty, clean, span):
    """Events pushed out of the window by lateness (they belong to the next one)."""
    present = {m.event_id for m in dirty}
    return {m.event_id for m in clean if m.event_id not in present
            and span[1] - m.event_ts < timedelta(minutes=31)}


def test_event_ids_are_stable_across_runs(world):
    from shop_sim.orders import OrderBook

    first = EventStream(OrderBook(world)).between(START, START + timedelta(hours=6))
    second = EventStream(OrderBook(world)).between(START, START + timedelta(hours=6))
    assert first == second
