"""Batch job: Shop API -> bronze.customers and bronze.products.

Two steps, on purpose:

1. extract  the API answers are written as-is to a JSON-lines file in the
            landing zone. That file is the raw archive: the load can be
            replayed without calling the API again.
2. load     Spark reads the file and appends the records to the Delta table.

The load is incremental. Each run asks the API only for records changed
since the newest `updated_at` already in bronze (minus a small safety
margin), and skips the versions it already holds. Bronze therefore keeps
every version of a customer or product, which the next layers need to
rebuild history (a price change, a customer who moved).
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .. import lake
from ..api_client import ShopApiClient
from ..config import Config
from . import new_run_id

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Entity:
    name: str
    key: str
    columns: tuple[str, ...]


ENTITIES = {
    "customers": Entity(
        name="customers",
        key="customer_id",
        columns=(
            "customer_id", "first_name", "last_name", "email", "phone", "city",
            "postal_code", "country", "created_at", "updated_at",
        ),
    ),
    "products": Entity(
        name="products",
        key="product_id",
        columns=(
            "product_id", "name", "category", "brand", "unit_price", "currency",
            "is_active", "created_at", "updated_at",
        ),
    ),
}

ISO_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"


def watermark(spark: SparkSession, table_path: str, lookback_minutes: int) -> str | None:
    """Newest `updated_at` in the table minus the margin, as an ISO string. None if empty."""
    if not lake.exists(spark, table_path):
        return None
    newest = F.max(F.expr("try_cast(updated_at AS TIMESTAMP)"))
    margin = F.expr(f"INTERVAL {int(lookback_minutes)} MINUTES")
    row = lake.read(spark, table_path).agg(F.date_format(newest - margin, ISO_FORMAT)).first()
    return row[0] if row else None


def extract(
    client: ShopApiClient, entity: Entity, updated_since: str | None, landing_dir: Path, run_id: str
) -> tuple[Path | None, int]:
    """Write the API records to <landing>/<entity>_<run_id>.jsonl. Returns (path, count)."""
    landing_dir.mkdir(parents=True, exist_ok=True)
    path = landing_dir / f"{entity.name}_{run_id}.jsonl"
    tmp_path = path.with_suffix(".jsonl.tmp")
    count = 0
    try:
        with open(tmp_path, "w", encoding="utf-8") as handle:
            for record in client.iter_records(entity.name, updated_since):
                handle.write(json.dumps(record, ensure_ascii=False) + "\n")
                count += 1
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise
    if count == 0:
        tmp_path.unlink()
        return None, 0
    # Rename only once complete: a failed extraction leaves no usable file behind.
    os.replace(tmp_path, path)
    return path, count


def read_landing(spark: SparkSession, path: str, entity: Entity, run_id: str) -> DataFrame:
    """One row per JSON line: the fields as text columns, plus the untouched line in `_raw`.

    Keeping `_raw` means a field the API adds tomorrow is not lost, even
    though it has no column yet.
    """
    schema = ", ".join(f"{column} STRING" for column in entity.columns)
    lines = spark.read.text(path).select(
        F.col("value").alias("_raw"),
        F.col("_metadata.file_name").alias("_source_file"),
    )
    parsed = lines.withColumn("_parsed", F.from_json("_raw", schema))
    return parsed.select(
        *[F.col(f"_parsed.{column}").alias(column) for column in entity.columns],
        "_raw",
        "_source_file",
        F.current_timestamp().alias("_ingested_at"),
        F.lit(run_id).alias("_run_id"),
    )


def only_new_versions(incoming: DataFrame, existing: DataFrame, key: str) -> DataFrame:
    """Drop the record versions (same key, same updated_at) already in the table."""
    known = existing.select(key, "updated_at").distinct()
    return incoming.join(known, on=[key, "updated_at"], how="left_anti")


def run_entity(
    spark: SparkSession,
    config: Config,
    entity: Entity,
    client: ShopApiClient,
    *,
    full: bool = False,
    lookback_minutes: int = 10,
) -> dict:
    started = time.monotonic()
    run_id = new_run_id()
    table_path = config.table("bronze", entity.name)
    since = None if full else watermark(spark, table_path, lookback_minutes)
    log.info("%s: extracting %s", entity.name, f"changes since {since}" if since else "everything")

    retries_before = client.retries
    path, extracted = extract(client, entity, since, config.api_landing(entity.name), run_id)
    loaded = 0
    if path is not None:
        incoming = read_landing(spark, str(path), entity, run_id)
        if lake.exists(spark, table_path):
            incoming = only_new_versions(incoming, lake.read(spark, table_path), entity.key)
        lake.append(incoming, table_path)
        loaded = lake.read(spark, table_path).where(F.col("_run_id") == run_id).count()

    return {
        "job": "bronze-api",
        "run_id": run_id,
        "entity": entity.name,
        "table": table_path,
        "updated_since": since,
        "extracted": extracted,
        "rows": loaded,
        "already_known": extracted - loaded,
        "api_retries": client.retries - retries_before,
        "landing_file": str(path) if path else None,
        "extracted_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "seconds": round(time.monotonic() - started, 1),
    }


def run(
    spark: SparkSession,
    config: Config,
    *,
    entities: list[str] | None = None,
    full: bool = False,
    lookback_minutes: int = 10,
    client: ShopApiClient | None = None,
) -> list[dict]:
    client = client or ShopApiClient(config.api_url)
    return [
        run_entity(spark, config, ENTITIES[name], client, full=full,
                   lookback_minutes=lookback_minutes)
        for name in (entities or list(ENTITIES))
    ]
