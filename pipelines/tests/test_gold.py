from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import pytest
from helpers import (
    api_bronze,
    created_event,
    customer_record,
    events_bronze,
    export_row,
    item,
    marketplace_bronze,
    product_ids,
    product_record,
    status_event,
)

from lakehouse import lake
from lakehouse.gold import build
from lakehouse.gold import job as gold_job
from lakehouse.silver import job as silver_job
from lakehouse.silver import marketplace, order_events, reference

WEB = "WEB-20261006-00001"
MKP = "MKP-20261006-00001"


def _silver_lines(spark, events=(), exports=(), known=("P0007", "P0011")):
    """Stacked silver order lines of both sources, built from bronze-shaped input."""
    ids = product_ids(spark, *known)
    clean, _, _ = order_events.build_events(events_bronze(spark, list(events)))
    web, _ = order_events.build_order_lines(clean, ids)
    mkp, _ = marketplace.build(marketplace_bronze(spark, list(exports)), ids)
    return web.unionByName(mkp)


# --- amounts -------------------------------------------------------------------------


def test_line_amounts_are_exact_decimals(spark):
    lines = _silver_lines(spark, events=[created_event(items=[
        item(1, "P0007", 2, 49.9, 10),     # 99.80 gross, 89.82 net
        item(2, "P0011", 1, 49.9, 15),     # 49.90 x 0.85 = 42.415 -> 42.42, not 42.41
        item(3, "P0011", 3, 19.9, 0),
    ])])
    rows = {r["line_number"]: r for r in build.build_order_lines(lines).collect()}
    assert (rows[1]["gross_amount"], rows[1]["discount_amount"], rows[1]["net_amount"]) == (
        Decimal("99.80"), Decimal("9.98"), Decimal("89.82"))
    assert (rows[2]["gross_amount"], rows[2]["discount_amount"], rows[2]["net_amount"]) == (
        Decimal("49.90"), Decimal("7.48"), Decimal("42.42"))
    assert (rows[3]["gross_amount"], rows[3]["discount_amount"], rows[3]["net_amount"]) == (
        Decimal("59.70"), Decimal("0.00"), Decimal("59.70"))
    assert build.build_order_lines(lines).columns == build.ORDER_LINE_COLUMNS


def test_an_order_totals_its_lines_and_adds_shipping(spark):
    lines = _silver_lines(spark, events=[
        created_event(items=[item(1, "P0007", 2, 49.9, 10), item(2, "P0011", 1, 19.9)],
                      total_amount=114.62),
        status_event(WEB, "paid", "2026-10-06T08:03:10.000Z"),
    ])
    (order,) = build.build_orders(lines).collect()
    assert order["lines_count"] == 2 and order["units"] == 3
    assert order["gross_amount"] == Decimal("119.70")
    assert order["discount_amount"] == Decimal("9.98")
    assert order["net_amount"] == Decimal("109.72")
    assert order["shipping_fee"] == Decimal("4.90")
    assert order["total_amount"] == Decimal("114.62")
    assert order["source_total_amount"] == Decimal("114.62")   # what the shop declared
    assert order["status"] == "paid" and order["paid_at"] == datetime(2026, 10, 6, 8, 3, 10)
    assert order["is_cancelled"] is False and order["is_returned"] is False
    assert build.build_orders(lines).columns == build.ORDER_COLUMNS


# --- business rules ---------------------------------------------------------------------


def test_the_order_date_is_the_day_in_france(spark):
    lines = _silver_lines(spark, events=[
        created_event("WEB-20261007-00001", ts="2026-10-06T22:30:00.000Z"),  # 00:30 in Paris
        created_event("WEB-20261006-00002", ts="2026-10-06T21:30:00.000Z"),  # 23:30 in Paris
    ])
    dates = {r["order_id"]: r["order_date"] for r in build.build_orders(lines).collect()}
    assert dates == {"WEB-20261007-00001": date(2026, 10, 7), "WEB-20261006-00002": date(2026, 10, 6)}


def test_cancelled_and_returned_orders_are_flagged(spark):
    lines = _silver_lines(spark, events=[
        created_event("WEB-1"), status_event("WEB-1", "cancelled", "2026-10-06T09:00:00.000Z"),
        created_event("WEB-2"),
        status_event("WEB-2", "paid", "2026-10-06T08:05:00.000Z"),
        status_event("WEB-2", "shipped", "2026-10-07T08:00:00.000Z"),
        status_event("WEB-2", "delivered", "2026-10-08T08:00:00.000Z"),
        status_event("WEB-2", "returned", "2026-10-12T08:00:00.000Z"),
    ])
    orders = {r["order_id"]: r for r in build.build_orders(lines).collect()}
    assert (orders["WEB-1"]["is_cancelled"], orders["WEB-1"]["is_returned"]) == (True, False)
    assert (orders["WEB-2"]["is_cancelled"], orders["WEB-2"]["is_returned"]) == (False, True)
    assert orders["WEB-2"]["delivered_at"] == datetime(2026, 10, 8, 8, 0)


def test_both_sales_channels_end_up_in_the_same_tables(spark):
    lines = _silver_lines(
        spark,
        events=[created_event(channel="mobile")],
        exports=[export_row(), export_row(ligne="2", ref_produit="P0011", quantite="1")],
    )
    orders = {r["order_id"]: r for r in build.build_orders(lines).collect()}
    assert set(orders) == {WEB, MKP}
    assert (orders[WEB]["source"], orders[WEB]["channel"]) == ("web_shop", "mobile")
    assert (orders[MKP]["source"], orders[MKP]["channel"]) == ("marketplace", "marketplace")
    assert orders[MKP]["lines_count"] == 2
    assert orders[MKP]["source_total_amount"] is None
    # 2 x 49.90 at -10% = 89.82, plus 1 x 49.90 at -10% = 44.91, free shipping.
    assert orders[MKP]["total_amount"] == Decimal("134.73")
    assert build.build_order_lines(lines).count() == 3


def test_gold_reference_tables_hold_the_current_version_only(spark):
    customers, _ = reference.build_customers(api_bronze(spark, "customers", [
        customer_record(1, "2026-10-01T10:00:00.000Z", city="Nice"),
        customer_record(1, "2026-10-06T08:30:00.000Z", city="Lyon", postal_code="69003"),
        customer_record(2, "2026-10-02T10:00:00.000Z"),
    ]))
    gold = {r["customer_id"]: r for r in build.build_customers(customers).collect()}
    assert len(gold) == 2
    assert (gold["C0000001"]["city"], gold["C0000001"]["postal_code"]) == ("Lyon", "69003")
    assert gold["C0000001"]["signup_date"] == date(2026, 1, 5)

    products, _ = reference.build_products(api_bronze(spark, "products", [
        product_record(7, "2025-03-01T00:00:00.000Z", unit_price=48.9),
        product_record(7, "2026-06-14T09:00:00.000Z", unit_price=52.9, category=None),
    ]))
    (product,) = build.build_products(products).collect()
    assert product["unit_price"] == Decimal("52.90")
    assert product["category"] == "Unknown"       # a labelled bucket instead of a blank
    assert product["brand"] == "Le Fou Blanc"


# --- the jobs, against real Delta tables ------------------------------------------------


def _load_bronze(spark, config):
    lake.append(api_bronze(spark, "customers", [customer_record(42)]),
                config.table("bronze", "customers"))
    lake.append(api_bronze(spark, "products", [product_record(7), product_record(11)]),
                config.table("bronze", "products"))
    lake.append(marketplace_bronze(spark, [
        export_row(), export_row(),                         # duplicate row
        export_row(ligne="2", ref_produit="P9999"),         # unknown product
    ]), config.table("bronze", "marketplace_orders"))
    paid = status_event(WEB, "paid", "2026-10-06T08:03:10.000Z")
    lake.append(events_bronze(spark, [
        created_event(items=[item(1, "P0007", 1, 49.9), item(2, "P0011", 2, 19.9)]),
        paid, paid,                                         # delivered twice
        '{"event_id": "broken',                             # truncated
    ]), config.table("bronze", "order_events"))


@pytest.mark.delta
def test_silver_then_gold_from_bronze_and_rerun_gives_the_same_result(spark, config):
    _load_bronze(spark, config)

    silver = silver_job.run(spark, config)
    assert silver["rows"] == {
        "customers": 1, "products": 2, "order_events": 2,
        "web_order_lines": 2, "marketplace_order_lines": 1, "rejects": 2,
    }
    assert silver["rejected"] == {
        "marketplace_orders.unknown_product": 1,
        "order_events.malformed_json": 1,
    }
    assert silver["duplicates_removed"] == {"marketplace_orders": 1, "order_events": 1}

    gold = gold_job.run(spark, config)
    assert gold["rows"] == {"order_lines": 3, "orders": 2, "customers": 1, "products": 2}
    assert gold["orders_by_source"] == {
        "web_shop": {"orders": 1, "total_amount": 94.6},      # 49.90 + 39.80 + 4.90 shipping
        "marketplace": {"orders": 1, "total_amount": 89.82},
    }
    assert (gold["first_order_date"], gold["last_order_date"]) == ("2026-10-06", "2026-10-06")

    # Rebuilding replaces the tables: same input, same output, no accumulation.
    again_silver = silver_job.run(spark, config)
    again_gold = gold_job.run(spark, config)
    assert again_silver["rows"] == silver["rows"]
    assert again_gold["rows"] == gold["rows"]
    assert lake.read(spark, config.table("gold", "orders")).count() == 2


@pytest.mark.delta
def test_layers_must_be_built_in_order(spark, config):
    with pytest.raises(gold_job.MissingSilverError, match="Run the silver job first"):
        gold_job.run(spark, config)
    with pytest.raises(silver_job.MissingBronzeError, match="customers, products"):
        silver_job.run(spark, config)

    lake.append(api_bronze(spark, "customers", [customer_record(42)]),
                config.table("bronze", "customers"))
    with pytest.raises(
        silver_job.MissingBronzeError,
        match="not loaded yet: products, marketplace_orders, order_events",
    ):
        silver_job.run(spark, config)
