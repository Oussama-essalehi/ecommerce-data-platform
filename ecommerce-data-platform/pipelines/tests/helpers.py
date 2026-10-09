"""Builders for small bronze-shaped DataFrames used by the silver and gold tests."""

from __future__ import annotations

import json
import uuid
from datetime import date, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from lakehouse import lake
from lakehouse.bronze.api import ENTITIES
from lakehouse.bronze.marketplace import SOURCE_COLUMNS

INGESTED_AT = datetime(2026, 10, 8, 6, 0, 0)

# --- marketplace export rows ---------------------------------------------------

EXPORT_ROW = {
    "numero_commande": "MKP-20261006-00001",
    "date_commande": "06/10/2026 09:12:44",
    "date_maj": "06/10/2026 09:14:02",
    "statut": "payée",
    "id_client": "C0000042",
    "ligne": "1",
    "ref_produit": "P0007",
    "quantite": "2",
    "prix_unitaire": "49,90",
    "remise_pct": "10",
    "frais_port": "0,00",
    "mode_paiement": "CB",
    "ville_livraison": "Nice",
    "code_postal": "06000",
}


def export_row(file_day: str = "2026-10-06", corrupt: str | None = None, **changes) -> dict:
    """One bronze row of a marketplace export; `changes` override the default columns."""
    return {**EXPORT_ROW, **changes, "_file": file_day, "_corrupt_record": corrupt}


def marketplace_bronze(spark: SparkSession, rows: list[dict]) -> DataFrame:
    schema = StructType(
        [StructField(c, StringType()) for c in SOURCE_COLUMNS]
        + [
            StructField("_corrupt_record", StringType()),
            StructField("_source_file", StringType()),
            StructField("_file_date", DateType()),
            StructField("_ingested_at", TimestampType()),
            StructField("_run_id", StringType()),
        ]
    )
    data = [
        (
            *[row.get(c) for c in SOURCE_COLUMNS],
            row.get("_corrupt_record"),
            f"marketplace_orders_{row['_file']}.csv",
            date.fromisoformat(row["_file"]),
            INGESTED_AT,
            "run-1",
        )
        for row in rows
    ]
    return spark.createDataFrame(data, schema)


# --- web shop events -------------------------------------------------------------


def item(line_number: int = 1, product_id: str = "P0007", quantity: int = 1,
         unit_price: float = 49.9, discount_pct: int = 0) -> dict:
    return {"line_number": line_number, "product_id": product_id, "quantity": quantity,
            "unit_price": unit_price, "discount_pct": discount_pct}


def created_event(order_id: str = "WEB-20261006-00001", ts: str = "2026-10-06T08:00:00.000Z",
                  items: list[dict] | None = None, **changes) -> dict:
    event = {
        "event_id": str(uuid.uuid5(uuid.NAMESPACE_OID, f"{order_id}|created")),
        "event_type": "order_created",
        "event_ts": ts,
        "schema_version": 1,
        "order_id": order_id,
        "customer_id": "C0000042",
        "channel": "web",
        "payment_method": "card",
        "currency": "EUR",
        "shipping": {"city": "Nice", "postal_code": "06000", "fee": 4.9},
        "items": items if items is not None else [item()],
        "total_amount": 54.8,
    }
    event.update(changes)
    return event


def status_event(order_id: str, status: str, ts: str, **changes) -> dict:
    event = {
        "event_id": str(uuid.uuid5(uuid.NAMESPACE_OID, f"{order_id}|{status}")),
        "event_type": "order_status_changed",
        "event_ts": ts,
        "schema_version": 1,
        "order_id": order_id,
        "status": status,
    }
    event.update(changes)
    return event


def events_bronze(spark: SparkSession, messages: list[dict | str]) -> DataFrame:
    """Bronze order_events rows. A dict is serialised to JSON; a string is used as-is."""
    schema = StructType(
        [
            StructField("key", StringType()),
            StructField("value", StringType()),
            StructField("topic", StringType()),
            StructField("partition", IntegerType()),
            StructField("offset", LongType()),
            StructField("kafka_timestamp", TimestampType()),
            StructField("_ingested_at", TimestampType()),
        ]
    )
    data = [
        (
            None,
            message if isinstance(message, str) else json.dumps(message, ensure_ascii=False),
            "shop.orders.v1",
            0,
            offset,
            INGESTED_AT,
            INGESTED_AT,
        )
        for offset, message in enumerate(messages)
    ]
    return spark.createDataFrame(data, schema)


# --- API records -------------------------------------------------------------------


def customer_record(number: int = 1, updated_at: str = "2026-10-01T10:00:00.000Z",
                    **changes) -> dict:
    record = {
        "customer_id": f"C{number:07d}",
        "first_name": "Inès",
        "last_name": "Lefèvre",
        "email": f"ines.lefevre{number}@courriel.example",
        "phone": "0639981234",
        "city": "Nice",
        "postal_code": "06000",
        "country": "FR",
        "created_at": "2026-01-05T08:00:00.000Z",
        "updated_at": updated_at,
    }
    record.update(changes)
    return record


def product_record(number: int = 7, updated_at: str = "2026-09-01T00:00:00.000Z",
                   **changes) -> dict:
    record = {
        "product_id": f"P{number:04d}",
        "name": "Jeu d'échecs en bois Le Fou Blanc",
        "category": "Livres & Jeux",
        "brand": "Le Fou Blanc",
        "unit_price": 48.9,
        "currency": "EUR",
        "is_active": True,
        "created_at": "2025-03-01T00:00:00.000Z",
        "updated_at": updated_at,
    }
    record.update(changes)
    return record


def api_bronze(spark: SparkSession, entity: str, records: list[dict],
               ingested_at: datetime = INGESTED_AT) -> DataFrame:
    """Bronze customers/products rows, with every field as text like the real table."""
    columns = ENTITIES[entity].columns
    schema = StructType(
        [StructField(c, StringType()) for c in columns]
        + [
            StructField("_raw", StringType()),
            StructField("_source_file", StringType()),
            StructField("_ingested_at", TimestampType()),
            StructField("_run_id", StringType()),
        ]
    )

    def text(value):
        if value is None or isinstance(value, str):
            return value
        return json.dumps(value)

    data = [
        (*[text(r.get(c)) for c in columns], json.dumps(r, ensure_ascii=False),
         f"{entity}_run-1.jsonl", ingested_at, "run-1")
        for r in records
    ]
    return spark.createDataFrame(data, schema)


def product_ids(spark: SparkSession, *ids: str) -> DataFrame:
    return spark.createDataFrame([(i,) for i in ids], "product_id string")


# --- a small but complete bronze layer --------------------------------------------------


def load_sample_bronze(spark: SparkSession, config) -> None:
    """Write the four bronze tables with a handful of rows, defects included.

    Gives one web shop order (2 lines, paid) and one marketplace order
    (1 valid line), plus a duplicate row, an unknown product, a re-delivered
    event and a truncated payload for silver to deal with.
    """
    web_order = "WEB-20261006-00001"
    lake.append(api_bronze(spark, "customers", [customer_record(42)]),
                config.table("bronze", "customers"))
    lake.append(api_bronze(spark, "products", [product_record(7), product_record(11)]),
                config.table("bronze", "products"))
    lake.append(marketplace_bronze(spark, [
        export_row(), export_row(),                         # duplicate row
        export_row(ligne="2", ref_produit="P9999"),         # unknown product
    ]), config.table("bronze", "marketplace_orders"))
    paid = status_event(web_order, "paid", "2026-10-06T08:03:10.000Z")
    lake.append(events_bronze(spark, [
        created_event(web_order, items=[item(1, "P0007", 1, 49.9), item(2, "P0011", 2, 19.9)]),
        paid, paid,                                         # delivered twice
        '{"event_id": "broken',                             # truncated
    ]), config.table("bronze", "order_events"))
