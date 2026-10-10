"""Daily batch of the e-commerce data platform.

Every night, once the previous business day is over:

    business_day ──► wait_for_marketplace_export ──► bronze_marketplace ─┐
                                                                         ├─► silver ─► gold ─► quality ─► warehouse_load ─► dbt_build ─► dbt_source_freshness
                                                      bronze_api ────────┘

Order events are not in this DAG: they are ingested continuously by the
`bronze-events` streaming service. Airflow orchestrates the batch side.
"""

from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param, task

from common.alerting import notify_failure

BUSINESS_TIME_ZONE = "Europe/Paris"

# How a task starts a Spark job or dbt. The jobs live in their own virtual
# environments, so their dependencies never clash with Airflow's.
LAKEHOUSE = os.environ.get(
    "LAKEHOUSE_CMD",
    "cd /opt/project/pipelines && /opt/venvs/lakehouse/bin/python -m lakehouse",
)
DBT = os.environ.get("DBT_CMD", "cd /opt/project/warehouse && /opt/venvs/dbt/bin/dbt")
LANDING_ROOT = Path(os.environ.get("LANDING_ROOT", "data/landing"))

default_args = {
    "owner": "data-platform",
    # Transient failures (the API is down, a container restarts) are retried
    # with a growing pause. Only when retries are used up does the alert go out.
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(minutes=45),
    "on_failure_callback": notify_failure,
}

with DAG(
    dag_id="ecommerce_daily",
    description="Daily batch: sources to bronze, silver, gold, quality gate, warehouse, dbt",
    doc_md=__doc__,
    schedule="0 3 * * *",  # 03:00 in Paris, every day
    start_date=pendulum.datetime(2026, 10, 1, tz=BUSINESS_TIME_ZONE),
    catchup=False,
    # A run rebuilds silver and gold: two at once would step on each other.
    max_active_runs=1,
    # Two Spark jobs side by side at most, to stay within a laptop's memory.
    max_active_tasks=2,
    default_args=default_args,
    params={
        "business_day": Param(
            None,
            type=["null", "string"],
            format="date",
            description="Day to process (YYYY-MM-DD). Leave empty for the day before the run.",
        ),
    },
    tags=["ecommerce", "batch"],
) as dag:

    @task
    def business_day(params=None, logical_date=None, dag_run=None) -> str:
        """The day this run loads: the one given when triggering, else yesterday in France."""
        if params and params.get("business_day"):
            return params["business_day"]
        run_time = logical_date or dag_run.run_after
        local_day = run_time.astimezone(ZoneInfo(BUSINESS_TIME_ZONE)).date()
        return (local_day - timedelta(days=1)).isoformat()

    @task.sensor(
        poke_interval=60,
        timeout=2 * 60 * 60,
        # "reschedule" frees the worker slot between two checks.
        mode="reschedule",
        retries=0,
    )
    def wait_for_marketplace_export(day: str) -> bool:
        """Wait for the partner's file. If it has not come after two hours, fail and alert."""
        return (LANDING_ROOT / "marketplace" / f"marketplace_orders_{day}.csv").exists()

    day = business_day()
    export_arrived = wait_for_marketplace_export(day)

    bronze_marketplace = BashOperator(
        task_id="bronze_marketplace",
        bash_command=LAKEHOUSE
        + " bronze-marketplace --date {{ ti.xcom_pull(task_ids='business_day') }}",
    )
    bronze_api = BashOperator(task_id="bronze_api", bash_command=f"{LAKEHOUSE} bronze-api")

    silver = BashOperator(task_id="silver", bash_command=f"{LAKEHOUSE} silver")
    gold = BashOperator(task_id="gold", bash_command=f"{LAKEHOUSE} gold")

    quality = BashOperator(
        task_id="quality",
        bash_command=f"{LAKEHOUSE} quality",
        # The gate: an error-level check fails this task, and nothing below
        # it runs. The same data would fail the same way, so no retry.
        retries=0,
    )

    warehouse_load = BashOperator(
        task_id="warehouse_load", bash_command=f"{LAKEHOUSE} warehouse-load"
    )
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{DBT} build",
        # A failing dbt test is not transient either.
        retries=0,
    )
    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness", bash_command=f"{DBT} source freshness", retries=0
    )

    export_arrived >> bronze_marketplace
    [bronze_marketplace, bronze_api] >> silver >> gold >> quality
    quality >> warehouse_load >> dbt_build >> dbt_source_freshness
