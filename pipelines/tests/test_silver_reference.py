from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from helpers import api_bronze, customer_record, product_record

from lakehouse.silver import reference


def _one(df):
    rows = df.collect()
    assert len(rows) == 1
    return rows[0]


# --- cleaning ---------------------------------------------------------------------


def test_customer_fields_are_typed_and_standardised(spark):
    bronze = api_bronze(spark, "customers", [
        customer_record(1, email="  INES.LEFEVRE1@COURRIEL.EXAMPLE ", city=" Nice "),
    ])
    customers, rejects = reference.build_customers(bronze)
    row = _one(customers)
    assert row["email"] == "ines.lefevre1@courriel.example"
    assert row["city"] == "Nice"
    assert row["postal_code"] == "06000"
    assert row["created_at"] == datetime(2026, 1, 5, 8, 0)
    assert row["updated_at"] == datetime(2026, 10, 1, 10, 0)
    assert rejects.count() == 0


def test_an_invalid_email_or_postal_code_becomes_null(spark):
    bronze = api_bronze(spark, "customers", [
        customer_record(1, email="ines.lefevre1courriel.example", postal_code=None),
        customer_record(2, email="", postal_code="6000"),
    ])
    customers, rejects = reference.build_customers(bronze)
    rows = {r["customer_id"]: r for r in customers.collect()}
    assert rows["C0000001"]["email"] is None and rows["C0000001"]["postal_code"] is None
    assert rows["C0000002"]["email"] is None and rows["C0000002"]["postal_code"] is None
    # The customers themselves are kept: a bad email does not make a bad customer.
    assert len(rows) == 2 and rejects.count() == 0


def test_product_fields_are_typed(spark):
    bronze = api_bronze(spark, "products", [product_record(7, brand="  ", is_active=False)])
    row = _one(reference.build_products(bronze)[0])
    assert row["unit_price"] == Decimal("48.90")
    assert row["is_active"] is False
    assert row["brand"] is None                      # blank text becomes NULL
    assert row["category"] == "Livres & Jeux"


def test_a_zero_price_is_treated_as_missing(spark):
    bronze = api_bronze(spark, "products", [product_record(7, unit_price=0.0)])
    assert _one(reference.build_products(bronze)[0])["unit_price"] is None


def test_records_without_key_or_date_are_rejected_with_a_reason(spark):
    bronze = api_bronze(spark, "customers", [
        customer_record(1),
        customer_record(2, customer_id=None),
        customer_record(3, updated_at="yesterday"),
    ])
    customers, rejects = reference.build_customers(bronze)
    assert [r["customer_id"] for r in customers.collect()] == ["C0000001"]
    found = {r["reason"]: r for r in rejects.collect()}
    assert set(found) == {"missing_key", "invalid_timestamp"}
    assert found["missing_key"]["source"] == "customers"
    assert '"last_name": "Lefèvre"' in found["missing_key"]["record"]  # the raw record is kept
    assert found["missing_key"]["source_ref"] == "customers_run-1.jsonl"


# --- history -----------------------------------------------------------------------


def test_versions_become_validity_periods(spark):
    bronze = api_bronze(spark, "products", [
        product_record(7, "2025-03-01T00:00:00.000Z", unit_price=48.9),
        product_record(7, "2026-06-14T09:00:00.000Z", unit_price=52.9),
        product_record(7, "2026-09-02T09:00:00.000Z", unit_price=50.9),
        product_record(8, "2025-03-01T00:00:00.000Z"),
    ])
    products = reference.build_products(bronze)[0]
    history = sorted(
        (r for r in products.collect() if r["product_id"] == "P0007"),
        key=lambda r: r["valid_from"],
    )
    assert [(r["unit_price"], r["valid_from"], r["valid_to"], r["is_current"]) for r in history] == [
        (Decimal("48.90"), datetime(2025, 3, 1), datetime(2026, 6, 14, 9), False),
        (Decimal("52.90"), datetime(2026, 6, 14, 9), datetime(2026, 9, 2, 9), False),
        (Decimal("50.90"), datetime(2026, 9, 2, 9), None, True),
    ]
    assert products.where("is_current").count() == 2  # exactly one current row per product


def test_the_first_known_version_is_valid_since_creation(spark):
    # First seen after a change: created in January, version dated October.
    bronze = api_bronze(spark, "customers", [customer_record(1, "2026-10-01T10:00:00.000Z")])
    row = _one(reference.build_customers(bronze)[0])
    assert row["valid_from"] == datetime(2026, 1, 5, 8, 0)
    assert row["valid_to"] is None and row["is_current"] is True


def test_the_same_version_ingested_twice_counts_once(spark):
    first = api_bronze(spark, "customers", [customer_record(1)], datetime(2026, 10, 8, 6))
    again = api_bronze(spark, "customers", [customer_record(1)], datetime(2026, 10, 9, 6))
    customers = reference.build_customers(first.unionByName(again))[0]
    assert customers.count() == 1
