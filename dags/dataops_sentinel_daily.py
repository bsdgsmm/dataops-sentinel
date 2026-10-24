"""Daily marketplace ETL with ingestion, quality gates, dbt and an operational report.

Schedule: 02:00 UTC every day for the previous day's data. The logical date
of a run is the UTC calendar date it processes (``data_interval_start``).
``catchup=False`` stops Airflow from creating a run for every day since the
start date when the DAG is unpaused; history is processed on purpose with
``airflow dags backfill``. ``max_active_runs=1`` keeps dates in order because
the row-count check compares each date with the dates before it.
"""

from __future__ import annotations

from datetime import date, timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.operators.python import get_current_context

from dataops_sentinel.config import Settings
from dataops_sentinel.errors import PermanentPipelineError
from dataops_sentinel.ingestion.schemas import LOAD_ORDER
from dataops_sentinel.orchestration import tasks

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=15),
}


def _context() -> tuple[date, str, int]:
    context = get_current_context()
    logical_date = context["data_interval_start"].date()
    return logical_date, context["run_id"], context["ti"].try_number


def _run(func, *args, **kwargs):
    """Fail fast on permanent errors so Airflow does not waste retries on them."""
    try:
        return func(*args, **kwargs)
    except PermanentPipelineError as exc:
        raise AirflowFailException(f"{type(exc).__name__}: {exc}") from exc


@dag(
    dag_id="dataops_sentinel_daily",
    description="Daily synthetic marketplace ETL with quality gates",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1),
    default_args=DEFAULT_ARGS,
    tags=["dataops", "synthetic"],
    params={
        "generate_synthetic_data": Param(True, type="boolean"),
        "scenario": Param("default", enum=["clean", "default", "dirty", "missing_column"]),
        "seed": Param(7, type="integer"),
    },
    doc_md=__doc__,
)
def dataops_sentinel_daily():
    @task(retries=0)
    def prepare_run() -> None:
        logical_date, run_id, _ = _context()
        settings = Settings.from_env()
        _run(tasks.ensure_schema, settings)
        tasks.start_run(settings, run_id, logical_date, "airflow")

    @task(retries=0)
    def land_source_files() -> str:
        from dataops_sentinel.synthetic.generator import generate_day, write_day

        context = get_current_context()
        logical_date = context["data_interval_start"].date()
        params = context["params"]
        settings = Settings.from_env()
        if not params["generate_synthetic_data"]:
            return "using existing landing files"
        folder = write_day(
            generate_day(params["seed"], logical_date, params["scenario"]),
            settings.landing_dir,
            params["seed"],
            params["scenario"],
        )
        return str(folder)

    def ingest(dataset: str):
        @task(task_id=f"ingest_{dataset}")
        def _ingest() -> dict:
            logical_date, run_id, try_number = _context()
            return _run(
                tasks.ingest_dataset, Settings.from_env(), dataset, logical_date, run_id, try_number
            )

        return _ingest()

    @task
    def quality_raw() -> None:
        logical_date, run_id, try_number = _context()
        _run(tasks.check_raw_quality, Settings.from_env(), logical_date, run_id, try_number)

    @task(execution_timeout=timedelta(minutes=30))
    def dbt_build() -> None:
        logical_date, run_id, try_number = _context()
        _run(tasks.transform, Settings.from_env(), logical_date, run_id, try_number)

    @task
    def quality_marts() -> None:
        logical_date, run_id, try_number = _context()
        _run(tasks.check_mart_quality, Settings.from_env(), logical_date, run_id, try_number)

    @task(trigger_rule="all_done", retries=1)
    def finalize_run() -> str:
        context = get_current_context()
        logical_date, run_id, _ = _context()
        settings = Settings.from_env()
        failed = [
            ti.task_id
            for ti in context["dag_run"].get_task_instances()
            if ti.state in {"failed", "upstream_failed"} and ti.task_id != "finalize_run"
        ]
        tasks.finish_run(
            settings,
            run_id,
            "failed" if failed else "success",
            f"failed tasks: {', '.join(sorted(failed))}" if failed else None,
        )
        from dataops_sentinel.database.connection import open_connection
        from dataops_sentinel.reporting.report import load_report_data, render_markdown

        with open_connection(settings.database) as conn:
            data = load_report_data(conn, logical_date, logical_date)
            generated_at = conn.execute("SELECT NOW()").fetchone()[0]
        path = settings.reports_dir / f"pipeline_report_{logical_date}.md"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            render_markdown(data, logical_date, logical_date, generated_at), encoding="utf-8"
        )
        if failed:
            raise AirflowFailException(f"run finished with failed tasks: {failed}")
        return str(path)

    prepared = prepare_run()
    landed = land_source_files()
    prepared >> landed

    ingested = [ingest(dataset) for dataset in LOAD_ORDER]
    raw_gate = quality_raw()
    landed >> ingested >> raw_gate
    raw_gate >> dbt_build() >> quality_marts() >> finalize_run()


dataops_sentinel_daily()
