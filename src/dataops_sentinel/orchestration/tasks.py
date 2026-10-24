"""Pipeline task implementations.

Every task:

* receives the logical date and the attempt number from its caller (Airflow
  or the CLI) instead of reading wall-clock time;
* records its start, end and any error in ``audit.task_runs``, in its own
  autocommitted statements so a failed attempt stays visible;
* is safe to run again for the same date.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import date
from typing import Any

from dataops_sentinel.config import Settings
from dataops_sentinel.database import repository
from dataops_sentinel.database.connection import (
    Connection,
    classify_database_error,
    open_connection,
)
from dataops_sentinel.errors import DataQualityError, SentinelError
from dataops_sentinel.ingestion.reader import read_source_file
from dataops_sentinel.ingestion.schemas import DATASETS
from dataops_sentinel.orchestration.dbt_runner import run_dbt
from dataops_sentinel.orchestration.faults import FaultInjector
from dataops_sentinel.validation import quality
from dataops_sentinel.validation.records import validate_file

logger = logging.getLogger(__name__)

ConnectionFactory = Callable[[], Any]


@contextmanager
def tracked(
    conn: Connection, run_id: str, task_name: str, try_number: int, logical_date: date
) -> Iterator[None]:
    repository.start_task_run(conn, run_id, task_name, try_number, logical_date)
    try:
        yield
    except BaseException as exc:
        repository.finish_task_run(conn, run_id, task_name, try_number, "failed", exc)
        raise
    repository.finish_task_run(conn, run_id, task_name, try_number, "success")


@contextmanager
def _database(settings: Settings, factory: ConnectionFactory | None) -> Iterator[Connection]:
    try:
        if factory is not None:
            conn = factory()
            try:
                yield conn
            finally:
                conn.close()
        else:
            with open_connection(settings.database) as conn:
                yield conn
    except SentinelError:
        raise
    except Exception as exc:
        if type(exc).__module__.startswith("psycopg"):
            raise classify_database_error(exc) from exc
        raise


def ensure_schema(settings: Settings, connection_factory: ConnectionFactory | None = None) -> None:
    from dataops_sentinel.database.connection import apply_schema

    with _database(settings, connection_factory) as conn:
        apply_schema(conn, settings.schema_path)


def start_run(
    settings: Settings,
    run_id: str,
    logical_date: date,
    trigger: str,
    connection_factory: ConnectionFactory | None = None,
) -> None:
    with _database(settings, connection_factory) as conn:
        repository.start_pipeline_run(conn, run_id, logical_date, trigger)


def finish_run(
    settings: Settings,
    run_id: str,
    status: str,
    error: str | None = None,
    connection_factory: ConnectionFactory | None = None,
) -> None:
    with _database(settings, connection_factory) as conn:
        repository.finish_pipeline_run(conn, run_id, status, error)


def ingest_dataset(
    settings: Settings,
    dataset: str,
    logical_date: date,
    run_id: str,
    try_number: int = 1,
    connection_factory: ConnectionFactory | None = None,
) -> dict[str, Any]:
    spec = DATASETS[dataset]
    task_name = f"ingest_{dataset}"
    injector = FaultInjector(settings.fault_injection, settings.state_dir)
    with (
        _database(settings, connection_factory) as conn,
        tracked(conn, run_id, task_name, try_number, logical_date),
    ):
        source = read_source_file(settings.landing_dir, logical_date, spec)
        result = validate_file(source)
        summary = repository.replace_partition(
            conn,
            spec,
            result,
            run_id=run_id,
            try_number=try_number,
            source_path=str(source.path),
            unexpected_columns=source.unexpected_columns,
            before_commit=lambda: injector.maybe_fail(task_name, logical_date),
        )
    logger.info(
        "Partition loaded",
        extra={
            "dataset": dataset,
            "logical_date": str(logical_date),
            "read": summary.records_read,
            "accepted": summary.records_accepted,
            "rejected": summary.records_rejected,
            "replaced_rows": summary.rows_replaced,
            "try_number": try_number,
        },
    )
    return {
        "dataset": dataset,
        "records_read": summary.records_read,
        "records_accepted": summary.records_accepted,
        "records_rejected": summary.records_rejected,
        "rows_replaced": summary.rows_replaced,
    }


def _quality_task(
    settings: Settings,
    stage: str,
    checks: Callable[[Connection, date, Any], list[quality.CheckResult]],
    logical_date: date,
    run_id: str,
    try_number: int,
    connection_factory: ConnectionFactory | None,
) -> list[quality.CheckResult]:
    task_name = f"quality_{stage}"
    with (
        _database(settings, connection_factory) as conn,
        tracked(conn, run_id, task_name, try_number, logical_date),
    ):
        results = checks(conn, logical_date, settings.thresholds)
        repository.save_quality_results(conn, run_id, logical_date, stage, results)
        for item in results:
            level = logging.WARNING if item.outcome.value in {"warn", "fail"} else logging.INFO
            logger.log(
                level,
                "Quality check %s: %s",
                item.name,
                item.outcome.value,
                extra={"severity": item.severity.value, "details": item.details},
            )
        failures = quality.blocking_failures(results)
        if failures:
            raise DataQualityError(
                f"{len(failures)} blocking quality check(s) failed for {logical_date}: "
                + ", ".join(item.name for item in failures),
                [item.name for item in failures],
            )
    return results


def check_raw_quality(
    settings: Settings,
    logical_date: date,
    run_id: str,
    try_number: int = 1,
    connection_factory: ConnectionFactory | None = None,
) -> list[quality.CheckResult]:
    return _quality_task(
        settings,
        "raw",
        quality.run_raw_checks,
        logical_date,
        run_id,
        try_number,
        connection_factory,
    )


def check_mart_quality(
    settings: Settings,
    logical_date: date,
    run_id: str,
    try_number: int = 1,
    connection_factory: ConnectionFactory | None = None,
) -> list[quality.CheckResult]:
    return _quality_task(
        settings,
        "marts",
        quality.run_mart_checks,
        logical_date,
        run_id,
        try_number,
        connection_factory,
    )


def transform(
    settings: Settings,
    logical_date: date,
    run_id: str,
    try_number: int = 1,
    connection_factory: ConnectionFactory | None = None,
    dbt: Callable[..., str] = run_dbt,
) -> None:
    injector = FaultInjector(settings.fault_injection, settings.state_dir)
    with (
        _database(settings, connection_factory) as conn,
        tracked(conn, run_id, "dbt_build", try_number, logical_date),
    ):
        injector.maybe_fail("dbt_build", logical_date)
        dbt(settings, logical_date, "build")
