"""Batch job: run every quality suite and record the results."""

from __future__ import annotations

import json
import logging
import os
import time
from collections import Counter
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .. import lake
from ..bronze import new_run_id
from ..config import Config
from . import CHECK_RESULTS_SCHEMA, ERROR, REJECT_HISTORY_SCHEMA

log = logging.getLogger(__name__)

# Great Expectations reports anonymous usage statistics unless told not to.
os.environ.setdefault("GX_ANALYTICS_ENABLED", "false")

PASSED, WARNED, FAILED = "passed", "warning", "failed"
SAMPLE_LIMIT = 300  # characters of offending values kept per check


def _context(config: Config):
    """A Great Expectations project stored under the quality folder.

    Keeping it on disk is what makes the HTML report (Data Docs) accumulate
    run after run.
    """
    import great_expectations as gx
    from great_expectations.data_context.types.base import ProgressBarsConfig

    root = config.quality_root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    context = gx.get_context(mode="file", project_root_dir=str(root))
    context.variables.progress_bars = ProgressBarsConfig(globally=False)
    context.variables.save()
    return context


def _text(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, float):
        return f"{value:g}"
    return str(value)


def _run_suite(context, suite, df: DataFrame, run_id: str, checked_at: datetime,
               build_docs: bool) -> list[dict]:
    """Validate one DataFrame and return one result row per check."""
    import great_expectations as gx

    checks = {check.name: check for check in suite.checks}
    if len(checks) != len(suite.checks):
        duplicated = [n for n, c in Counter(c.name for c in suite.checks).items() if c > 1]
        raise ValueError(f"{suite.name}: duplicate check names {duplicated}")

    source = context.data_sources.add_or_update_spark("lake")
    asset_name = suite.name.replace(".", "_")
    try:
        asset = source.get_asset(asset_name)
    except LookupError:
        asset = source.add_dataframe_asset(asset_name)
    try:
        batch = asset.get_batch_definition("whole_table")
    except KeyError:
        batch = asset.add_batch_definition_whole_dataframe("whole_table")

    expectation_suite = gx.ExpectationSuite(name=suite.name)
    for check in suite.checks:
        # Tag each expectation so its result can be matched back to the check.
        check.expectation.meta = {"check": check.name, "dimension": check.dimension,
                                  "severity": check.severity}
        check.expectation.notes = check.description
        expectation_suite.add_expectation(check.expectation)
    expectation_suite = context.suites.add_or_update(expectation_suite)

    validation = context.validation_definitions.add_or_update(
        gx.ValidationDefinition(name=suite.name, data=batch, suite=expectation_suite)
    )
    actions = [gx.checkpoint.UpdateDataDocsAction(name="update_data_docs")] if build_docs else []
    checkpoint = context.checkpoints.add_or_update(
        gx.Checkpoint(name=suite.name, validation_definitions=[validation],
                      actions=actions, result_format="BASIC")
    )
    outcome = checkpoint.run(
        batch_parameters={"dataframe": df},
        run_id=gx.core.RunIdentifier(run_name=run_id, run_time=checked_at),
    )
    (validation_result,) = outcome.run_results.values()

    rows = []
    for result in validation_result.results:
        configuration = result.expectation_config
        check = checks[configuration.meta["check"]]
        details = result.result or {}
        success = bool(result.success)
        observed = details.get("observed_value")
        sample = details.get("partial_unexpected_list")
        if not details and not success:
            # The check could not run at all (missing column, wrong type...).
            observed = "check could not be evaluated: " + json.dumps(
                result.exception_info, default=str)[:SAMPLE_LIMIT]
        rows.append(
            {
                "run_id": run_id,
                "checked_at": checked_at,
                "layer": suite.layer,
                "table_name": suite.table,
                "check_name": check.name,
                "dimension": check.dimension,
                "severity": check.severity,
                "description": check.description,
                "expectation": configuration.type,
                "column_name": configuration.kwargs.get("column")
                or ", ".join(
                    configuration.kwargs.get("column_list")
                    or [c for c in (configuration.kwargs.get("column_A"),
                                    configuration.kwargs.get("column_B")) if c]
                ) or None,
                "success": success,
                "status": PASSED if success else (FAILED if check.severity == ERROR else WARNED),
                "element_count": details.get("element_count"),
                "unexpected_count": details.get("unexpected_count"),
                "unexpected_percent": details.get("unexpected_percent"),
                "observed_value": _text(observed),
                "sample": json.dumps(sample, default=str, ensure_ascii=False)[:SAMPLE_LIMIT]
                if sample else None,
            }
        )
    missing = set(checks) - {row["check_name"] for row in rows}
    if missing:
        raise RuntimeError(f"{suite.name}: no result returned for checks {sorted(missing)}")
    return sorted(rows, key=lambda row: list(checks).index(row["check_name"]))


def _reject_history(spark: SparkSession, config: Config, run_id: str,
                    checked_at: datetime) -> DataFrame:
    """Today's count of rejected records per source and reason.

    silver.rejects is rebuilt on every run and only shows the present. This
    snapshot is what lets the number of anomalies be followed over time.
    """
    path = config.table("silver", "rejects")
    if not lake.exists(spark, path):
        return spark.createDataFrame([], REJECT_HISTORY_SCHEMA)
    return (
        lake.read(spark, path).groupBy("source", "reason").agg(F.count("*").alias("rejected"))
        .select(
            F.lit(run_id).alias("run_id"),
            # Parsed in the session time zone, which is UTC.
            F.lit(checked_at.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp").alias("checked_at"),
            "source", "reason", "rejected",
        )
    )


def docs_index(config: Config) -> str:
    return str(config.quality_root.resolve() / "gx" / "uncommitted" / "data_docs"
               / "local_site" / "index.html")


def run(spark: SparkSession, config: Config, *, build_docs: bool = True,
        suites: list | None = None) -> dict:
    from .suites import build_suites

    started = time.monotonic()
    run_id = new_run_id()
    # Timezone-aware on purpose: Spark reads a naive datetime as local time.
    checked_at = datetime.now(timezone.utc).replace(microsecond=0)
    context = _context(config)

    rows: list[dict] = []
    for suite in suites if suites is not None else build_suites():
        suite_started = time.monotonic()
        try:
            df = suite.prepare(spark, config)
            suite_rows = _run_suite(context, suite, df, run_id, checked_at, build_docs)
        except Exception as error:  # noqa: BLE001 - one broken suite must not hide the others
            log.error("%s: could not be checked (%s)", suite.name, error)
            suite_rows = [
                {
                    "run_id": run_id, "checked_at": checked_at, "layer": suite.layer,
                    "table_name": suite.table, "check_name": "suite_ran",
                    "dimension": "validity", "severity": ERROR,
                    "description": "the table could be read and checked",
                    "expectation": None, "column_name": None, "success": False,
                    "status": FAILED, "element_count": None, "unexpected_count": None,
                    "unexpected_percent": None,
                    "observed_value": f"{type(error).__name__}: {error}"[:SAMPLE_LIMIT],
                    "sample": None,
                }
            ]
        rows.extend(suite_rows)
        counts = Counter(row["status"] for row in suite_rows)
        log.info("%s: %d passed, %d warnings, %d failed (%.0fs)", suite.name, counts[PASSED],
                 counts[WARNED], counts[FAILED], time.monotonic() - suite_started)

    lake.append(spark.createDataFrame(rows, CHECK_RESULTS_SCHEMA),
                config.table("quality", "check_results"))
    lake.append(_reject_history(spark, config, run_id, checked_at),
                config.table("quality", "reject_history"))

    problems = [row for row in rows if row["status"] != PASSED]
    for row in problems:
        log.log(
            logging.ERROR if row["status"] == FAILED else logging.WARNING,
            "%s %s.%s: %s (observed: %s%s)",
            row["status"].upper(), row["layer"], row["table_name"], row["description"],
            row["observed_value"]
            if row["observed_value"] is not None
            else f"{row['unexpected_count']} rows, {row['unexpected_percent'] or 0:.3g}%",
            f", e.g. {row['sample']}" if row["sample"] else "",
        )

    counts = Counter(row["status"] for row in rows)
    return {
        "job": "quality",
        "run_id": run_id,
        "checks": len(rows),
        "passed": counts[PASSED],
        "warnings": counts[WARNED],
        "failed": counts[FAILED],
        "problems": [
            {"status": r["status"], "table": f"{r['layer']}.{r['table_name']}",
             "check": r["check_name"], "observed": r["observed_value"],
             "unexpected_count": r["unexpected_count"]}
            for r in problems
        ],
        "report": docs_index(config) if build_docs else None,
        "seconds": round(time.monotonic() - started, 1),
    }
