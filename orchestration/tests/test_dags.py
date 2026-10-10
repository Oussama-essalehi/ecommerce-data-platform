from __future__ import annotations

from datetime import timedelta

import pytest
from airflow.dag_processing.dagbag import DagBag
from conftest import DAGS_FOLDER

from common.alerting import notify_failure


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAGS_FOLDER))


@pytest.fixture(scope="module")
def dag(dagbag):
    return dagbag.dags["ecommerce_daily"]


def test_dags_import_without_error(dagbag):
    assert dagbag.import_errors == {}
    assert set(dagbag.dags) == {"ecommerce_daily"}


def test_schedule_is_nightly_in_french_time(dag):
    assert dag.schedule == "0 3 * * *" or "0 3 * * *" in str(dag.timetable.summary)
    assert dag.timezone.name == "Europe/Paris"
    assert dag.catchup is False
    assert dag.max_active_runs == 1


def test_tasks_run_in_the_order_of_the_data_flow(dag):
    upstream = {task.task_id: set(task.upstream_task_ids) for task in dag.tasks}
    assert upstream == {
        "business_day": set(),
        "wait_for_marketplace_export": {"business_day"},
        "bronze_marketplace": {"wait_for_marketplace_export"},
        "bronze_api": set(),
        "silver": {"bronze_marketplace", "bronze_api"},
        "gold": {"silver"},
        "quality": {"gold"},
        # Nothing reaches the warehouse unless the quality gate passed.
        "warehouse_load": {"quality"},
        "dbt_build": {"warehouse_load"},
        "dbt_source_freshness": {"dbt_build"},
    }


def test_every_task_alerts_on_failure(dag):
    for task in dag.tasks:
        callbacks = task.on_failure_callback
        callbacks = callbacks if isinstance(callbacks, (list, tuple)) else [callbacks]
        assert notify_failure in callbacks, task.task_id


def test_transient_steps_retry_and_deterministic_ones_do_not(dag):
    retries = {task.task_id: task.retries for task in dag.tasks}
    for task_id in ("bronze_api", "bronze_marketplace", "silver", "gold", "warehouse_load"):
        assert retries[task_id] == 2, task_id
    # Checks and tests give the same verdict on the same data: retrying is noise.
    for task_id in ("quality", "dbt_build", "dbt_source_freshness",
                    "wait_for_marketplace_export"):
        assert retries[task_id] == 0, task_id

    api = dag.get_task("bronze_api")
    assert api.retry_delay == timedelta(minutes=2)
    assert api.retry_exponential_backoff
    assert api.execution_timeout == timedelta(minutes=45)


def test_the_sensor_waits_without_holding_a_worker(dag):
    sensor = dag.get_task("wait_for_marketplace_export")
    assert sensor.mode == "reschedule"
    assert sensor.timeout == 2 * 60 * 60
    assert sensor.poke_interval == 60


def test_commands_target_the_right_jobs(dag):
    commands = {
        task.task_id: task.bash_command for task in dag.tasks if hasattr(task, "bash_command")
    }
    assert commands["bronze_api"].endswith("-m lakehouse bronze-api")
    assert "bronze-marketplace --date {{ ti.xcom_pull(task_ids='business_day') }}" in commands[
        "bronze_marketplace"]
    assert commands["quality"].endswith("-m lakehouse quality")
    assert commands["warehouse_load"].endswith("-m lakehouse warehouse-load")
    assert commands["dbt_build"].endswith("dbt build")
    assert commands["dbt_source_freshness"].endswith("dbt source freshness")
