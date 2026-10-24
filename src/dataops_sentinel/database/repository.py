"""Idempotent writes for raw partitions and audit records.

Idempotency strategy: one dataset partition is the pair (dataset,
logical_date). Loading a partition deletes whatever is stored for that pair
and inserts the newly validated rows, all inside one transaction, together
with the rejected rows and the batch audit record. Re-running a date
therefore replaces the partition instead of appending to it, and a failure
at any point rolls the whole partition back to its previous state.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any

from dataops_sentinel.database.connection import Connection
from dataops_sentinel.ingestion.schemas import DatasetSpec
from dataops_sentinel.validation.records import ValidationResult


@dataclass(frozen=True)
class LoadSummary:
    dataset: str
    logical_date: date
    records_read: int
    records_accepted: int
    records_rejected: int
    rows_replaced: int


def _insert_sql(spec: DatasetSpec) -> str:
    columns = [*spec.column_names, "logical_date", "run_id"]
    placeholders = ", ".join(["%s"] * len(columns))
    return f"INSERT INTO {spec.table} ({', '.join(columns)}) VALUES ({placeholders})"


def replace_partition(
    conn: Connection,
    spec: DatasetSpec,
    result: ValidationResult,
    *,
    run_id: str,
    try_number: int,
    source_path: str,
    unexpected_columns: Sequence[str] = (),
    before_commit: Callable[[], None] | None = None,
) -> LoadSummary:
    """Atomically replace one (dataset, logical_date) partition.

    ``before_commit`` runs after all statements and before the commit; it is
    the hook used to demonstrate that a failure at that point leaves the
    previously loaded data untouched.
    """
    logical_date = result.logical_date
    rows = [
        [record[name] for name in spec.column_names] + [logical_date, run_id]
        for record in result.accepted
    ]
    rejects = [
        [
            spec.name,
            logical_date,
            record.row_number,
            record.business_key,
            [reason.value for reason in record.reasons],
            list(record.messages),
            record.raw_json(),
            run_id,
        ]
        for record in result.rejected
    ]
    with conn.transaction():
        cursor = conn.execute(f"DELETE FROM {spec.table} WHERE logical_date = %s", (logical_date,))
        replaced = max(getattr(cursor, "rowcount", 0) or 0, 0)
        conn.execute(
            "DELETE FROM raw.rejected_records WHERE dataset = %s AND logical_date = %s",
            (spec.name, logical_date),
        )
        with conn.cursor() as cur:
            if rows:
                cur.executemany(_insert_sql(spec), rows)
            if rejects:
                cur.executemany(
                    "INSERT INTO raw.rejected_records (dataset, logical_date, row_number, "
                    "business_key, reasons, messages, raw_record, run_id) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s)",
                    rejects,
                )
        conn.execute(
            "INSERT INTO audit.ingestion_batches (dataset, logical_date, run_id, try_number, "
            "source_path, records_read, records_accepted, records_rejected, reject_reasons, "
            "unexpected_columns) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s) "
            "ON CONFLICT (dataset, logical_date) DO UPDATE SET run_id = EXCLUDED.run_id, "
            "try_number = EXCLUDED.try_number, source_path = EXCLUDED.source_path, "
            "records_read = EXCLUDED.records_read, records_accepted = EXCLUDED.records_accepted, "
            "records_rejected = EXCLUDED.records_rejected, "
            "reject_reasons = EXCLUDED.reject_reasons, "
            "unexpected_columns = EXCLUDED.unexpected_columns, loaded_at = NOW()",
            (
                spec.name,
                logical_date,
                run_id,
                try_number,
                source_path,
                result.records_read,
                len(result.accepted),
                len(result.rejected),
                json.dumps(dict(result.reject_reasons), sort_keys=True),
                list(unexpected_columns),
            ),
        )
        if before_commit is not None:
            before_commit()
    return LoadSummary(
        dataset=spec.name,
        logical_date=logical_date,
        records_read=result.records_read,
        records_accepted=len(result.accepted),
        records_rejected=len(result.rejected),
        rows_replaced=replaced,
    )


def start_pipeline_run(conn: Connection, run_id: str, logical_date: date, trigger: str) -> None:
    conn.execute(
        "INSERT INTO audit.pipeline_runs (run_id, logical_date, trigger, status, started_at) "
        "VALUES (%s, %s, %s, 'running', NOW()) "
        "ON CONFLICT (run_id) DO UPDATE SET trigger = EXCLUDED.trigger, status = 'running', "
        "started_at = NOW(), finished_at = NULL, error = NULL",
        (run_id, logical_date, trigger),
    )


def finish_pipeline_run(conn: Connection, run_id: str, status: str, error: str | None) -> None:
    conn.execute(
        "UPDATE audit.pipeline_runs SET status = %s, finished_at = NOW(), error = %s "
        "WHERE run_id = %s",
        (status, error, run_id),
    )


def start_task_run(
    conn: Connection, run_id: str, task_name: str, try_number: int, logical_date: date
) -> None:
    conn.execute(
        "INSERT INTO audit.task_runs (run_id, task_name, try_number, logical_date, status, "
        "started_at) VALUES (%s, %s, %s, %s, 'running', NOW()) "
        "ON CONFLICT (run_id, task_name, try_number) DO UPDATE SET status = 'running', "
        "started_at = NOW(), finished_at = NULL, error_type = NULL, error = NULL",
        (run_id, task_name, try_number, logical_date),
    )


def finish_task_run(
    conn: Connection,
    run_id: str,
    task_name: str,
    try_number: int,
    status: str,
    error: BaseException | None = None,
) -> None:
    conn.execute(
        "UPDATE audit.task_runs SET status = %s, finished_at = NOW(), error_type = %s, "
        "error = %s WHERE run_id = %s AND task_name = %s AND try_number = %s",
        (
            status,
            type(error).__name__ if error else None,
            str(error)[:2000] if error else None,
            run_id,
            task_name,
            try_number,
        ),
    )


def save_quality_results(
    conn: Connection, run_id: str, logical_date: date, stage: str, results: Sequence[Any]
) -> None:
    with conn.transaction():
        conn.execute(
            "DELETE FROM audit.quality_results WHERE logical_date = %s AND stage = %s",
            (logical_date, stage),
        )
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO audit.quality_results (logical_date, stage, check_name, severity, "
                "outcome, observed, threshold, details, run_id) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                [
                    [
                        logical_date,
                        stage,
                        item.name,
                        item.severity.value,
                        item.outcome.value,
                        item.observed,
                        item.threshold,
                        item.details,
                        run_id,
                    ]
                    for item in results
                ],
            )
