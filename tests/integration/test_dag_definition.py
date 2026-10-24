"""Parses the DAG with a real Airflow installation (skipped when Airflow is absent)."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest

airflow = pytest.importorskip("airflow")

DAGS = Path(__file__).resolve().parents[2] / "dags"


@pytest.fixture(scope="module")
def dag():
    from airflow.models import DagBag

    bag = DagBag(dag_folder=str(DAGS), include_examples=False)
    assert bag.import_errors == {}
    return bag.get_dag("dataops_sentinel_daily")


def test_task_graph(dag) -> None:
    assert set(dag.task_ids) == {
        "prepare_run",
        "land_source_files",
        "ingest_customers",
        "ingest_products",
        "ingest_orders",
        "ingest_payments",
        "quality_raw",
        "dbt_build",
        "quality_marts",
        "finalize_run",
    }
    assert dag.get_task("quality_raw").upstream_task_ids == {
        "ingest_customers",
        "ingest_products",
        "ingest_orders",
        "ingest_payments",
    }
    assert dag.get_task("dbt_build").upstream_task_ids == {"quality_raw"}
    assert dag.get_task("finalize_run").trigger_rule == "all_done"


def test_scheduling_and_retry_settings(dag) -> None:
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert str(dag.timezone) in {"UTC", "Timezone('UTC')"}
    ingest = dag.get_task("ingest_orders")
    assert ingest.retries == 2
    assert ingest.retry_delay == timedelta(minutes=1)
    assert ingest.execution_timeout == timedelta(minutes=15)
    assert dag.get_task("prepare_run").retries == 0
