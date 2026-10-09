"""Batch job: marketplace CSV exports -> bronze.marketplace_orders.

One file per business day lands in <landing>/marketplace. The job loads the
files of the requested days and *replaces* those days in the table, so a
rerun, a backfill or a corrected file never creates duplicates.
"""

from __future__ import annotations

import logging
import re
import time
from datetime import date, timedelta
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from .. import lake
from ..config import Config
from . import new_run_id

log = logging.getLogger(__name__)

TABLE = "marketplace_orders"
FILE_PATTERN = re.compile(r"^marketplace_orders_(\d{4}-\d{2}-\d{2})\.csv$")

# The columns of the export, in file order. All read as text.
SOURCE_COLUMNS = [
    "numero_commande", "date_commande", "date_maj", "statut", "id_client",
    "ligne", "ref_produit", "quantite", "prix_unitaire", "remise_pct",
    "frais_port", "mode_paiement", "ville_livraison", "code_postal",
]
CORRUPT_COLUMN = "_corrupt_record"


class MissingFilesError(RuntimeError):
    """A requested day has no file in the landing zone."""


def available_files(landing_dir: Path) -> dict[date, Path]:
    """Export files present in the landing zone, by business day."""
    files: dict[date, Path] = {}
    if landing_dir.is_dir():
        for path in landing_dir.iterdir():
            match = FILE_PATTERN.match(path.name)
            if match:
                files[date.fromisoformat(match.group(1))] = path
    return dict(sorted(files.items()))


def select_files(
    available: dict[date, Path], start: date, end: date
) -> tuple[dict[date, Path], list[date]]:
    """Files for every day from start to end (both included), and the days without one."""
    selected: dict[date, Path] = {}
    missing: list[date] = []
    day = start
    while day <= end:
        if day in available:
            selected[day] = available[day]
        else:
            missing.append(day)
        day += timedelta(days=1)
    return selected, missing


def read_files(spark: SparkSession, paths: list[str]) -> DataFrame:
    """Read export files as raw text columns.

    `enforceSchema=false` makes Spark compare each file's header with the
    expected columns and fail on a mismatch: if the partner renames or
    reorders a column, the job stops instead of silently shifting data.
    Rows with the wrong number of fields are kept, with the original line
    in `_corrupt_record`.
    """
    schema = StructType(
        [StructField(name, StringType()) for name in SOURCE_COLUMNS]
        + [StructField(CORRUPT_COLUMN, StringType())]
    )
    return (
        spark.read.format("csv")
        .schema(schema)
        .option("header", True)
        .option("sep", ";")
        .option("encoding", "UTF-8")
        .option("enforceSchema", False)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_COLUMN)
        .load(paths)
    )


def add_metadata(df: DataFrame, run_id: str) -> DataFrame:
    file_name = F.col("_metadata.file_name")
    return (
        df.withColumn("_source_file", file_name)
        .withColumn(
            "_file_date",
            F.regexp_extract(file_name, r"(\d{4}-\d{2}-\d{2})", 1).try_cast("date"),
        )
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_run_id", F.lit(run_id))
    )


def days_predicate(days: list[date]) -> str:
    literals = ", ".join(f"DATE'{day.isoformat()}'" for day in days)
    return f"_file_date IN ({literals})"


def run(
    spark: SparkSession,
    config: Config,
    *,
    start: date | None = None,
    end: date | None = None,
    all_files: bool = False,
    skip_missing: bool = False,
) -> dict:
    """Load the files of [start, end], or every file present when `all_files`."""
    started = time.monotonic()
    run_id = new_run_id()
    available = available_files(config.marketplace_landing)

    if all_files:
        selected, missing = available, []
    else:
        if start is None or end is None:
            raise ValueError("start and end are required unless all_files is set")
        selected, missing = select_files(available, start, end)
        if missing and not skip_missing:
            raise MissingFilesError(
                f"no marketplace export in {config.marketplace_landing} for: "
                + ", ".join(day.isoformat() for day in missing)
            )

    summary = {
        "job": "bronze-marketplace",
        "run_id": run_id,
        "table": config.table("bronze", TABLE),
        "files": len(selected),
        "missing_days": [day.isoformat() for day in missing],
        "rows": 0,
    }
    if selected:
        days = list(selected)
        df = add_metadata(read_files(spark, [str(path) for path in selected.values()]), run_id)
        lake.replace_where(spark, df, config.table("bronze", TABLE), days_predicate(days))
        summary["first_day"] = days[0].isoformat()
        summary["last_day"] = days[-1].isoformat()
        summary["rows"] = (
            lake.read(spark, config.table("bronze", TABLE)).where(F.col("_run_id") == run_id).count()
        )
    else:
        log.warning("no file to load from %s", config.marketplace_landing)
    summary["seconds"] = round(time.monotonic() - started, 1)
    return summary
