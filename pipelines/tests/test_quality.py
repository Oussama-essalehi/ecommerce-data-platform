from __future__ import annotations

from collections import Counter
from decimal import Decimal
from pathlib import Path

import pytest
from helpers import load_sample_bronze

from lakehouse import lake
from lakehouse.gold import job as gold_job
from lakehouse.quality import DIMENSIONS, ERROR, WARNING, runner, suites
from lakehouse.quality.suites import Suite
from lakehouse.silver import job as silver_job

# --- the catalogue of checks (no Spark needed) ------------------------------------------


def test_every_check_is_named_once_and_classified():
    catalogue = suites.build_suites()
    assert len({suite.name for suite in catalogue}) == len(catalogue)
    for suite in catalogue:
        names = [check.name for check in suite.checks]
        assert len(set(names)) == len(names), f"duplicate check name in {suite.name}"
        for check in suite.checks:
            assert check.dimension in DIMENSIONS
            assert check.severity in (ERROR, WARNING)
            assert check.description


def test_every_dimension_is_covered_and_freshness_never_blocks():
    checks = [check for suite in suites.build_suites() for check in suite.checks]
    assert {check.dimension for check in checks} == set(DIMENSIONS)
    assert all(c.severity == WARNING for c in checks if c.dimension == "freshness")


# --- running a suite on a DataFrame -------------------------------------------------------


def _orders(spark, rows):
    return spark.createDataFrame(
        rows, "order_id STRING, customer_id STRING, status STRING, total_amount DECIMAL(12,2)"
    )


def _suite(prepare) -> Suite:
    return Suite("test", "orders", prepare, [
        suites.has_rows(),
        suites.not_null("order_id"),
        suites.unique("order_id"),
        suites.not_null("customer_id", at_least=0.75, severity=WARNING),
        suites.in_set("status", ["paid", "shipped"]),
        suites.between("total_amount", 0, None),
    ])


def _run(spark, config, suite):
    summary_rows = runner._run_suite(  # noqa: SLF001 - the unit under test
        runner._context(config), suite, suite.prepare(spark, config),  # noqa: SLF001
        "run-1", runner.datetime(2026, 10, 9, 12, 0, tzinfo=runner.timezone.utc), False,
    )
    return {row["check_name"]: row for row in summary_rows}


def test_clean_data_passes_every_check(spark, config):
    clean = lambda spark, config: _orders(spark, [  # noqa: E731
        ("WEB-1", "C1", "paid", Decimal("10.00")), ("WEB-2", "C2", "shipped", Decimal("0.00")),
    ])
    results = _run(spark, config, _suite(clean))
    assert list(results) == [
        "has_rows", "order_id_not_null", "order_id_unique", "customer_id_not_null",
        "status_known_value", "total_amount_in_range",
    ]                                                   # one row per check, in suite order
    assert {row["status"] for row in results.values()} == {"passed"}
    assert results["has_rows"]["observed_value"] == "2"
    assert results["order_id_unique"]["element_count"] == 2
    assert results["order_id_unique"]["column_name"] == "order_id"


def test_each_broken_rule_is_reported_with_counts_and_examples(spark, config):
    dirty = lambda spark, config: _orders(spark, [  # noqa: E731
        ("WEB-1", "C1", "paid", Decimal("10.00")),
        ("WEB-1", None, "teleported", Decimal("-5.00")),     # duplicate id, 3 problems
        ("WEB-3", None, "shipped", Decimal("7.00")),
        (None, "C4", "paid", Decimal("3.00")),
    ])
    results = _run(spark, config, _suite(dirty))

    assert results["has_rows"]["status"] == "passed"
    assert results["order_id_not_null"]["status"] == "failed"
    assert results["order_id_not_null"]["unexpected_count"] == 1
    assert results["order_id_unique"]["status"] == "failed"
    assert results["order_id_unique"]["unexpected_count"] == 2     # both copies of WEB-1
    assert results["status_known_value"]["status"] == "failed"
    assert "teleported" in results["status_known_value"]["sample"]
    assert results["total_amount_in_range"]["unexpected_count"] == 1
    # 2 of 4 customers missing: below the 75% asked for, but only a warning.
    missing = results["customer_id_not_null"]
    assert (missing["status"], missing["severity"], missing["success"]) == ("warning", "warning", False)
    assert missing["unexpected_percent"] == 50.0


def test_a_threshold_tolerates_what_it_says(spark, config):
    one_in_four_missing = lambda spark, config: _orders(spark, [  # noqa: E731
        ("WEB-1", "C1", "paid", Decimal("1")), ("WEB-2", "C2", "paid", Decimal("1")),
        ("WEB-3", "C3", "paid", Decimal("1")), ("WEB-4", None, "paid", Decimal("1")),
    ])
    results = _run(spark, config, _suite(one_in_four_missing))
    assert results["customer_id_not_null"]["status"] == "passed"   # exactly 75% filled
    assert results["customer_id_not_null"]["unexpected_count"] == 1


def test_a_check_on_a_missing_column_fails_instead_of_passing(spark, config):
    without_status = lambda spark, config: _orders(spark, [  # noqa: E731
        ("WEB-1", "C1", "paid", Decimal("1")),
    ]).drop("status")
    results = _run(spark, config, _suite(without_status))
    assert results["status_known_value"]["status"] == "failed"
    assert "could not be evaluated" in results["status_known_value"]["observed_value"]
    assert results["order_id_unique"]["status"] == "passed"        # the others still ran


# --- the job, on a small lake built by the real jobs ------------------------------------------


@pytest.fixture()
def sample_lake(spark, config):
    load_sample_bronze(spark, config)
    silver_job.run(spark, config)
    gold_job.run(spark, config)


@pytest.mark.delta
def test_the_job_reports_stores_and_keeps_history(spark, config, sample_lake):
    summary = runner.run(spark, config)

    problems = {(p["table"], p["check"]): p["status"] for p in summary["problems"]}
    assert problems == {
        # The sample data dates from early October 2026: stale by now.
        ("bronze.freshness", "order_events_fresh"): "warning",
        ("bronze.freshness", "customers_fresh"): "warning",
        ("bronze.freshness", "marketplace_export_fresh"): "warning",
        ("gold.orders_freshness", "orders_fresh"): "warning",
        # 1 of the 3 marketplace rows and 1 of the 4 events are rejected: far
        # above the usual rate, and above the 5% that blocks the pipeline.
        ("silver.reject_rates", "reject_rate_usual"): "warning",
        ("silver.reject_rates", "reject_rate_acceptable"): "failed",
    }
    assert summary["failed"] == 1 and summary["warnings"] == 5
    assert summary["checks"] == summary["passed"] + 6

    stored = lake.read(spark, config.table("quality", "check_results"))
    assert stored.count() == summary["checks"]
    assert Counter(r["status"] for r in stored.collect()) == Counter(
        passed=summary["passed"], warning=5, failed=1
    )
    reconciled = stored.where("check_name = 'total_matches_declared_total'").first()
    assert (reconciled["layer"], reconciled["dimension"], reconciled["status"]) == (
        "gold", "consistency", "passed")
    assert stored.select("checked_at").distinct().count() == 1

    rejects = lake.read(spark, config.table("quality", "reject_history"))
    assert sorted((r["source"], r["reason"], r["rejected"]) for r in rejects.collect()) == [
        ("marketplace_orders", "unknown_product", 1),
        ("order_events", "malformed_json", 1),
    ]

    assert Path(summary["report"]).is_file()                       # the HTML report exists

    # A second run adds to the history instead of replacing it.
    again = runner.run(spark, config, build_docs=False)
    assert again["report"] is None
    history = lake.read(spark, config.table("quality", "check_results"))
    assert history.count() == 2 * summary["checks"]
    assert history.select("run_id").distinct().count() == 2
    assert lake.read(spark, config.table("quality", "reject_history")).count() == 4


@pytest.mark.delta
def test_a_wrong_total_is_caught(spark, config, sample_lake):
    orders = lake.read(spark, config.table("gold", "orders"))
    tampered = orders.withColumn("total_amount", orders.total_amount + Decimal("5.00"))
    lake.overwrite(tampered.localCheckpoint(), config.table("gold", "orders"))

    gold_orders = [s for s in suites.build_suites() if s.name == "gold.orders"]
    summary = runner.run(spark, config, build_docs=False, suites=gold_orders)

    failed = {p["check"] for p in summary["problems"] if p["status"] == "failed"}
    # Both rules about totals object: it no longer equals net plus shipping,
    # and it no longer matches what the shop declared.
    assert failed == {"total_is_net_plus_shipping", "total_matches_declared_total"}


@pytest.mark.delta
def test_a_suite_that_cannot_run_is_an_error_and_does_not_stop_the_others(spark, config, sample_lake):
    def unreadable(spark, config):
        raise FileNotFoundError("table folder is gone")

    broken = Suite("gold", "ghost", unreadable, [suites.has_rows()])
    products = [s for s in suites.build_suites() if s.name == "gold.products"]
    summary = runner.run(spark, config, build_docs=False, suites=[broken, *products])

    assert summary["failed"] == 1
    (problem,) = summary["problems"]
    assert (problem["table"], problem["check"]) == ("gold.ghost", "suite_ran")
    assert "table folder is gone" in problem["observed"]
    assert summary["passed"] == len(products[0].checks)
