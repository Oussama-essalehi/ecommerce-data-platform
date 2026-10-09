from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal

from helpers import created_event, events_bronze, item, product_ids, status_event

from lakehouse.silver import ORDER_LINE_COLUMNS, order_events

ORDER = "WEB-20261006-00001"


def _events(spark, messages):
    events, rejects, redelivered = order_events.build_events(events_bronze(spark, messages))
    return events, rejects.collect(), redelivered.count()


def _lines(spark, messages, known=("P0007", "P0011")):
    events, _, _ = order_events.build_events(events_bronze(spark, messages))
    lines, rejects = order_events.build_order_lines(events, product_ids(spark, *known))
    return lines.collect(), rejects.collect()


# --- the event log -------------------------------------------------------------------


def test_payloads_are_parsed_into_typed_columns(spark):
    events, rejects, _ = _events(spark, [
        created_event(items=[item(1, "P0007", 2, 49.9, 10), item(2, "P0011", 1, 19.9)]),
        status_event(ORDER, "paid", "2026-10-06T08:03:10.500Z"),
    ])
    assert rejects == []
    rows = {r["event_type"]: r for r in events.collect()}

    created = rows["order_created"]
    assert created["event_ts"] == datetime(2026, 10, 6, 8, 0)
    assert created["customer_id"] == "C0000042" and created["channel"] == "web"
    assert created["shipping_postal_code"] == "06000"
    assert created["shipping_fee"] == Decimal("4.90")
    assert created["total_amount"] == Decimal("54.80")
    assert [(i["product_id"], i["quantity"], i["unit_price"]) for i in created["items"]] == [
        ("P0007", 2, Decimal("49.90")), ("P0011", 1, Decimal("19.90")),
    ]
    assert (created["kafka_partition"], created["kafka_offset"]) == (0, 0)

    paid = rows["order_status_changed"]
    assert paid["status"] == "paid"
    assert paid["event_ts"] == datetime(2026, 10, 6, 8, 3, 10, 500000)
    assert paid["items"] is None


def test_a_truncated_payload_is_rejected_not_half_read(spark):
    whole = json.dumps(created_event(items=[item(1), item(2, "P0011")]))
    # Cut inside the items array: the order id is readable, the order is not.
    truncated = whole[: whole.index('"line_number": 2')]
    events, rejects, _ = _events(spark, [truncated, status_event(ORDER, "paid", "2026-10-06T08:03:10.000Z")])
    assert [r["event_type"] for r in events.collect()] == ["order_status_changed"]
    (reject,) = rejects
    assert (reject["source"], reject["reason"]) == ("order_events", "malformed_json")
    assert reject["record"] == truncated                 # the raw payload is kept
    assert reject["source_ref"] == "shop.orders.v1:0:0"  # where to find it in Kafka


def test_unusable_events_are_rejected_with_a_reason(spark):
    _, rejects, _ = _events(spark, [
        "not json at all",
        created_event(event_id=None),
        created_event("WEB-2", items=[]),
        status_event("WEB-3", "teleported", "2026-10-06T09:00:00.000Z"),
        status_event("WEB-4", "paid", "last tuesday"),
        {**status_event("WEB-5", "paid", "2026-10-06T09:00:00.000Z"), "event_type": "order_archived"},
    ])
    assert sorted(r["reason"] for r in rejects) == [
        "malformed_json", "missing_field", "missing_field", "missing_field",
        "unknown_event_type", "unknown_status",
    ]


def test_a_redelivered_event_is_kept_once(spark):
    paid = status_event(ORDER, "paid", "2026-10-06T08:03:10.000Z")
    events, rejects, redelivered = _events(spark, [created_event(), paid, paid, paid])
    assert events.count() == 2 and redelivered == 2 and rejects == []
    # The first delivery is the one that stays.
    assert events.where("event_type = 'order_status_changed'").first()["kafka_offset"] == 1


# --- order lines -----------------------------------------------------------------------


def test_an_order_and_its_statuses_fold_into_one_row_per_line(spark):
    lines, rejects = _lines(spark, [
        created_event(items=[item(1, "P0007", 2, 49.9, 10), item(2, "P0011", 1, 19.9)]),
        status_event(ORDER, "paid", "2026-10-06T08:03:10.000Z"),
        status_event(ORDER, "shipped", "2026-10-07T09:30:00.000Z"),
    ])
    assert rejects == []
    assert sorted((r["line_number"], r["product_id"], r["quantity"]) for r in lines) == [
        (1, "P0007", 2), (2, "P0011", 1),
    ]
    for line in lines:
        assert (line["source"], line["channel"]) == ("web_shop", "web")
        assert line["order_ts"] == datetime(2026, 10, 6, 8, 0)
        assert line["status"] == "shipped" and line["status_ts"] == datetime(2026, 10, 7, 9, 30)
        assert line["paid_at"] == datetime(2026, 10, 6, 8, 3, 10)
        assert line["shipped_at"] == datetime(2026, 10, 7, 9, 30)
        assert line["delivered_at"] is None
        assert line["source_total_amount"] == Decimal("54.80")


def test_output_has_the_shared_order_line_columns(spark):
    events, _, _ = order_events.build_events(events_bronze(spark, [created_event()]))
    lines, _ = order_events.build_order_lines(events, product_ids(spark, "P0007"))
    assert lines.columns == ORDER_LINE_COLUMNS


def test_statuses_are_ordered_by_when_they_happened_not_when_they_arrived(spark):
    # "shipped" reaches Kafka before "paid" (a late event) and before the order itself.
    lines, _ = _lines(spark, [
        status_event(ORDER, "shipped", "2026-10-07T09:30:00.000Z"),
        status_event(ORDER, "paid", "2026-10-06T08:03:10.000Z"),
        created_event(),
    ])
    assert lines[0]["status"] == "shipped"
    assert lines[0]["paid_at"] == datetime(2026, 10, 6, 8, 3, 10)


def test_an_order_without_status_event_is_just_created(spark):
    (line,), _ = _lines(spark, [created_event()])
    assert line["status"] == "created" and line["status_ts"] == line["order_ts"]


def test_an_order_without_customer_is_kept(spark):
    (line,), rejects = _lines(spark, [created_event(customer_id=None)])
    assert line["customer_id"] is None and rejects == []


def test_a_status_for_an_unknown_order_is_reported_as_orphan(spark):
    lines, rejects = _lines(spark, [
        created_event(),
        status_event("WEB-20261006-00099", "paid", "2026-10-06T10:00:00.000Z"),
    ])
    assert len(lines) == 1
    (reject,) = rejects
    assert reject["reason"] == "orphan_status_event"
    assert json.loads(reject["record"])["order_id"] == "WEB-20261006-00099"
    assert reject["source_ref"] == "0:1"


def test_a_bad_line_is_rejected_without_losing_the_rest_of_the_order(spark):
    lines, rejects = _lines(spark, [created_event(items=[
        item(1, "P0007"),
        item(2, "P9999"),                 # unknown product
        item(3, "P0011", quantity=0),     # impossible quantity
        item(4, "P0011", discount_pct=120),
    ])])
    assert [r["line_number"] for r in lines] == [1]
    assert sorted(r["reason"] for r in rejects) == [
        "invalid_amount", "invalid_quantity", "unknown_product",
    ]
    assert all(r["source_ref"] == ORDER for r in rejects)
