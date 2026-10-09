"""Batch job: rebuild the gold layer from silver."""

from __future__ import annotations

import logging
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .. import lake
from ..config import Config
from . import build

log = logging.getLogger(__name__)

SILVER_INPUTS = ["customers", "products", "web_order_lines", "marketplace_order_lines"]


class MissingSilverError(RuntimeError):
    """Gold cannot be built before silver."""


def _write(spark: SparkSession, config: Config, table: str, df: DataFrame) -> int:
    path = config.table("gold", table)
    lake.overwrite(df, path)
    rows = lake.read(spark, path).count()
    log.info("gold.%s: %d rows", table, rows)
    return rows


def run(spark: SparkSession, config: Config) -> dict:
    started = time.monotonic()
    missing = [t for t in SILVER_INPUTS if not lake.exists(spark, config.table("silver", t))]
    if missing:
        raise MissingSilverError(
            "silver tables not built yet: " + ", ".join(missing) + ". Run the silver job first."
        )
    silver = {t: lake.read(spark, config.table("silver", t)) for t in SILVER_INPUTS}
    lines = silver["web_order_lines"].unionByName(silver["marketplace_order_lines"])

    rows = {
        "order_lines": _write(spark, config, "order_lines", build.build_order_lines(lines)),
        "orders": _write(spark, config, "orders", build.build_orders(lines)),
        "customers": _write(spark, config, "customers", build.build_customers(silver["customers"])),
        "products": _write(spark, config, "products", build.build_products(silver["products"])),
    }

    orders = lake.read(spark, config.table("gold", "orders"))
    by_source = {
        r["source"]: {"orders": r["orders"], "total_amount": float(r["total_amount"])}
        for r in orders.groupBy("source")
        .agg(F.count("*").alias("orders"), F.sum("total_amount").alias("total_amount"))
        .collect()
    }
    span = orders.agg(F.min("order_date"), F.max("order_date")).first()
    return {
        "job": "gold",
        "rows": rows,
        "orders_by_source": by_source,
        "first_order_date": str(span[0]) if span[0] else None,
        "last_order_date": str(span[1]) if span[1] else None,
        "seconds": round(time.monotonic() - started, 1),
    }
