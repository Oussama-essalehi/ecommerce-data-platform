"""A quick look at what each layer of the lake holds."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from . import bronze, gold, lake, silver
from .config import Config

LAYERS = ["bronze", "silver", "gold"]
TABLES = {"bronze": bronze.TABLES, "silver": silver.TABLES, "gold": gold.TABLES}

# Per table: (label, aggregate expression) pairs shown next to the row count.
DETAILS = {
    "bronze.customers": [
        ("distinct customers", "count(DISTINCT customer_id)"),
        ("newest updated_at", "max(updated_at)"),
        ("last ingested at", "max(_ingested_at)"),
    ],
    "bronze.products": [
        ("distinct products", "count(DISTINCT product_id)"),
        ("newest updated_at", "max(updated_at)"),
        ("last ingested at", "max(_ingested_at)"),
    ],
    "bronze.marketplace_orders": [
        ("files", "count(DISTINCT _source_file)"),
        ("first day", "min(_file_date)"),
        ("last day", "max(_file_date)"),
        ("last ingested at", "max(_ingested_at)"),
    ],
    "bronze.order_events": [
        ("partitions", "count(DISTINCT partition)"),
        ("newest kafka_timestamp", "max(kafka_timestamp)"),
        ("last ingested at", "max(_ingested_at)"),
    ],
    "silver.customers": [("current customers", "count_if(is_current)")],
    "silver.products": [("current products", "count_if(is_current)")],
    "silver.order_events": [("newest event_ts", "max(event_ts)")],
    "silver.web_order_lines": [("orders", "count(DISTINCT order_id)")],
    "silver.marketplace_order_lines": [("orders", "count(DISTINCT order_id)")],
    "silver.rejects": [("distinct reasons", "count(DISTINCT reason)")],
    "gold.orders": [
        ("first order date", "min(order_date)"),
        ("last order date", "max(order_date)"),
        ("total amount", "sum(total_amount)"),
    ],
    "gold.order_lines": [("units sold", "sum(quantity)")],
}


def collect(spark: SparkSession, config: Config, layers: list[str] | None = None) -> list[dict]:
    report = []
    for layer in layers or LAYERS:
        for table in TABLES[layer]:
            name = f"{layer}.{table}"
            path = config.table(layer, table)
            if not lake.exists(spark, path):
                report.append({"table": name, "exists": False})
                continue
            details = DETAILS.get(name, [])
            row = (
                lake.read(spark, path)
                .agg(
                    F.count("*").alias("rows"),
                    *[F.expr(expression).cast("string").alias(f"d{i}")
                      for i, (_, expression) in enumerate(details)],
                )
                .first()
            )
            report.append(
                {
                    "table": name,
                    "exists": True,
                    "rows": row["rows"],
                    **{label: row[f"d{i}"] for i, (label, _) in enumerate(details)},
                }
            )
    return report


def render(report: list[dict]) -> str:
    lines = []
    for entry in report:
        if not entry["exists"]:
            lines.append(f"{entry['table']}: not created yet")
            continue
        lines.append(f"{entry['table']}: {entry['rows']:,} rows")
        for label, value in entry.items():
            if label not in ("table", "exists", "rows"):
                lines.append(f"    {label}: {value}")
    return "\n".join(lines)
