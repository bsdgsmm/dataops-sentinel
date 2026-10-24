"""Operational report built from the audit tables.

The report only shows values read from ``audit.*``: run status, logical
date, records read/accepted/rejected, quality outcomes, task errors, attempt
numbers and measured durations. Nothing is computed from assumptions.
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ReportData:
    runs: list[dict[str, Any]]
    batches: list[dict[str, Any]]
    quality: list[dict[str, Any]]
    tasks: list[dict[str, Any]]


QUERIES = {
    "runs": (
        "SELECT run_id, logical_date, trigger, status, started_at, finished_at, "
        "EXTRACT(EPOCH FROM finished_at - started_at) AS duration_seconds, error "
        "FROM audit.pipeline_runs WHERE logical_date BETWEEN %(start)s AND %(end)s "
        "ORDER BY logical_date, started_at"
    ),
    "batches": (
        "SELECT logical_date, dataset, records_read, records_accepted, records_rejected, "
        "try_number FROM audit.ingestion_batches WHERE logical_date BETWEEN %(start)s AND %(end)s "
        "ORDER BY logical_date, dataset"
    ),
    "quality": (
        "SELECT logical_date, stage, check_name, severity, outcome, observed, details "
        "FROM audit.quality_results WHERE logical_date BETWEEN %(start)s AND %(end)s "
        "ORDER BY logical_date, stage, check_name"
    ),
    "tasks": (
        "SELECT logical_date, run_id, task_name, try_number, status, "
        "EXTRACT(EPOCH FROM finished_at - started_at) AS duration_seconds, error_type, error "
        "FROM audit.task_runs WHERE logical_date BETWEEN %(start)s AND %(end)s "
        "ORDER BY logical_date, started_at, task_name, try_number"
    ),
}


def _rows(conn: Any, query: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    cursor = conn.execute(query, params)
    columns = [
        column.name if hasattr(column, "name") else column[0] for column in cursor.description
    ]
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def load_report_data(conn: Any, start: date, end: date) -> ReportData:
    params = {"start": start, "end": end}
    return ReportData(**{name: _rows(conn, sql, params) for name, sql in QUERIES.items()})


def _fmt(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S UTC")
    if isinstance(value, float | Decimal):
        return f"{float(value):.2f}"
    return str(value).replace("|", "\\|").replace("\n", " ")


def _table(headers: Sequence[str], rows: Sequence[Sequence[Any]]) -> str:
    if not rows:
        return "_No records._\n"
    lines = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    lines += ["| " + " | ".join(_fmt(value) for value in row) + " |" for row in rows]
    return "\n".join(lines) + "\n"


def render_markdown(data: ReportData, start: date, end: date, generated_at: datetime) -> str:
    out = [
        "# DataOps Sentinel - pipeline report",
        "",
        f"Logical dates {start} to {end}. Generated {_fmt(generated_at)} from the audit tables.",
        "All source data is synthetic.",
        "",
        "## Runs",
        "",
        _table(
            ["Logical date", "Run", "Trigger", "Status", "Duration (s)", "Error"],
            [
                [
                    r["logical_date"],
                    r["run_id"],
                    r["trigger"],
                    r["status"],
                    r["duration_seconds"],
                    r["error"],
                ]
                for r in data.runs
            ],
        ),
        "## Records",
        "",
        _table(
            [
                "Logical date",
                "Dataset",
                "Read",
                "Accepted",
                "Rejected",
                "Reject %",
                "Loaded on try",
            ],
            [
                [
                    b["logical_date"],
                    b["dataset"],
                    b["records_read"],
                    b["records_accepted"],
                    b["records_rejected"],
                    100 * b["records_rejected"] / b["records_read"] if b["records_read"] else 0.0,
                    b["try_number"],
                ]
                for b in data.batches
            ],
        ),
    ]

    problems = [q for q in data.quality if q["outcome"] in {"warn", "fail"}]
    totals: dict[str, int] = defaultdict(int)
    for item in data.quality:
        totals[item["outcome"]] += 1
    out += [
        "## Data quality",
        "",
        "Outcomes: " + (", ".join(f"{k}={v}" for k, v in sorted(totals.items())) or "none") + ".",
        "",
        _table(
            ["Logical date", "Stage", "Check", "Severity", "Outcome", "Observed", "Details"],
            [
                [
                    q["logical_date"],
                    q["stage"],
                    q["check_name"],
                    q["severity"],
                    q["outcome"],
                    q["observed"],
                    q["details"],
                ]
                for q in problems
            ],
        ),
    ]

    failed_tasks = [t for t in data.tasks if t["status"] == "failed"]
    retried = defaultdict(list)
    for t in data.tasks:
        retried[(t["run_id"], t["task_name"])].append(t)
    retry_rows = [
        [attempts[0]["logical_date"], name, len(attempts), attempts[-1]["status"]]
        for (_run, name), attempts in retried.items()
        if len(attempts) > 1
    ]
    out += [
        "## Task errors",
        "",
        _table(
            ["Logical date", "Task", "Try", "Error type", "Error"],
            [
                [t["logical_date"], t["task_name"], t["try_number"], t["error_type"], t["error"]]
                for t in failed_tasks
            ],
        ),
        "## Repeated attempts",
        "",
        "Tasks that ran more than once within the same run (Airflow retries or manual re-runs).",
        "",
        _table(["Logical date", "Task", "Attempts", "Final status"], retry_rows),
    ]
    return "\n".join(out)


def main(argv: list[str] | None = None) -> int:
    from dataops_sentinel.config import Settings
    from dataops_sentinel.database.connection import open_connection
    from dataops_sentinel.logging_config import configure_logging

    settings = Settings.from_env()
    configure_logging(settings.log_level)
    parser = argparse.ArgumentParser(description="Write the Markdown pipeline report.")
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    with open_connection(settings.database) as conn:
        data = load_report_data(conn, args.start, args.end)
        generated_at = conn.execute("SELECT NOW()").fetchone()[0]
    output = args.output or settings.reports_dir / f"pipeline_report_{args.start}_{args.end}.md"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(render_markdown(data, args.start, args.end, generated_at), encoding="utf-8")
    logger.info("Report written", extra={"path": str(output), "runs": len(data.runs)})
    return 0


if __name__ == "__main__":
    sys.exit(main())
