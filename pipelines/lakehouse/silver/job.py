"""Batch job: rebuild the whole silver layer from bronze."""

from __future__ import annotations

import logging
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .. import lake
from ..bronze import TABLES as BRONZE_TABLES
from ..config import Config
from . import REJECT_COLUMNS, marketplace, order_events, reference, union_all

log = logging.getLogger(__name__)


class MissingBronzeError(RuntimeError):
    """Silver cannot be built before every bronze table has been loaded once."""


def _read_bronze(spark: SparkSession, config: Config) -> dict[str, DataFrame]:
    missing = [t for t in BRONZE_TABLES if not lake.exists(spark, config.table("bronze", t))]
    if missing:
        raise MissingBronzeError(
            "bronze tables not loaded yet: " + ", ".join(missing)
            + ". Run the bronze jobs first (bronze-api, bronze-marketplace, bronze-events)."
        )
    return {t: lake.read(spark, config.table("bronze", t)) for t in BRONZE_TABLES}


def _write(spark: SparkSession, config: Config, table: str, df: DataFrame) -> int:
    path = config.table("silver", table)
    lake.overwrite(df, path)
    rows = lake.read(spark, path).count()
    log.info("silver.%s: %d rows", table, rows)
    return rows


def run(spark: SparkSession, config: Config) -> dict:
    started = time.monotonic()
    bronze = _read_bronze(spark, config)
    rows: dict[str, int] = {}
    rejects: list[DataFrame] = []

    # Reference data first: orders are validated against the product list.
    customers, customer_rejects = reference.build_customers(bronze["customers"])
    products, product_rejects = reference.build_products(bronze["products"])
    rows["customers"] = _write(spark, config, "customers", customers)
    rows["products"] = _write(spark, config, "products", products)
    rejects += [customer_rejects, product_rejects]
    # Read back what was just written: later steps then start from the
    # stored table instead of recomputing it from bronze.
    product_ids = lake.read(spark, config.table("silver", "products")).select("product_id")

    events, event_rejects, redelivered = order_events.build_events(bronze["order_events"])
    rows["order_events"] = _write(spark, config, "order_events", events)
    redelivered_events = redelivered.count()
    rejects.append(event_rejects)

    clean_events = lake.read(spark, config.table("silver", "order_events"))
    web_lines, web_rejects = order_events.build_order_lines(clean_events, product_ids)
    rows["web_order_lines"] = _write(spark, config, "web_order_lines", web_lines)
    rejects.append(web_rejects)

    bronze_marketplace = bronze["marketplace_orders"]
    marketplace_lines, marketplace_rejects = marketplace.build(bronze_marketplace, product_ids)
    rows["marketplace_order_lines"] = _write(
        spark, config, "marketplace_order_lines", marketplace_lines
    )
    rejects.append(marketplace_rejects)
    duplicate_rows = bronze_marketplace.count() - marketplace.parse(bronze_marketplace).count()

    rows["rejects"] = _write(spark, config, "rejects", union_all(rejects).select(*REJECT_COLUMNS))
    by_reason = (
        lake.read(spark, config.table("silver", "rejects"))
        .groupBy("source", "reason").count().orderBy("source", "reason").collect()
    )

    return {
        "job": "silver",
        "rows": rows,
        "rejected": {f"{r['source']}.{r['reason']}": r["count"] for r in by_reason},
        "duplicates_removed": {
            "marketplace_orders": duplicate_rows,
            "order_events": redelivered_events,
        },
        "seconds": round(time.monotonic() - started, 1),
    }
