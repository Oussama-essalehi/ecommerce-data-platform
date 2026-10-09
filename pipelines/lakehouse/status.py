"""A quick look at what the bronze layer holds."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from . import lake
from .bronze import TABLES
from .config import Config

# Per table: (label, aggregate expression) pairs shown next to the row count.
DETAILS = {
    "customers": [
        ("distinct customers", "count(DISTINCT customer_id)"),
        ("newest updated_at", "max(updated_at)"),
    ],
    "products": [
        ("distinct products", "count(DISTINCT product_id)"),
        ("newest updated_at", "max(updated_at)"),
    ],
    "marketplace_orders": [
        ("files", "count(DISTINCT _source_file)"),
        ("first day", "min(_file_date)"),
        ("last day", "max(_file_date)"),
    ],
    "order_events": [
        ("partitions", "count(DISTINCT partition)"),
        ("newest kafka_timestamp", "max(kafka_timestamp)"),
    ],
}


def collect(spark: SparkSession, config: Config) -> list[dict]:
    report = []
    for table in TABLES:
        path = config.table("bronze", table)
        if not lake.exists(spark, path):
            report.append({"table": table, "exists": False})
            continue
        details = DETAILS[table]
        row = (
            lake.read(spark, path)
            .agg(
                F.count("*").alias("rows"),
                F.max("_ingested_at").cast("string").alias("last_ingested_at"),
                *[F.expr(expression).cast("string").alias(f"d{i}")
                  for i, (_, expression) in enumerate(details)],
            )
            .first()
        )
        report.append(
            {
                "table": table,
                "exists": True,
                "rows": row["rows"],
                "last_ingested_at": row["last_ingested_at"],
                **{label: row[f"d{i}"] for i, (label, _) in enumerate(details)},
            }
        )
    return report


def render(report: list[dict]) -> str:
    lines = []
    for entry in report:
        if not entry["exists"]:
            lines.append(f"bronze.{entry['table']}: not created yet")
            continue
        lines.append(f"bronze.{entry['table']}: {entry['rows']:,} rows")
        for label, value in entry.items():
            if label not in ("table", "exists", "rows"):
                lines.append(f"    {label.replace('_', ' ')}: {value}")
    return "\n".join(lines)
