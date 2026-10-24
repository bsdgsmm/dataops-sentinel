"""Render a PNG summary of pipeline runs from the audit tables and marts.

Only values read from PostgreSQL are plotted. Usage:

    python scripts/render_run_chart.py --start 2026-01-01 --end 2026-01-09 \
        --output docs/images/pipeline_run_summary.png
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any

logger = logging.getLogger("render_run_chart")

QUERIES = {
    "records": (
        "SELECT logical_date, SUM(records_accepted) AS accepted, SUM(records_rejected) AS rejected "
        "FROM audit.ingestion_batches WHERE logical_date BETWEEN %(start)s AND %(end)s "
        "GROUP BY logical_date ORDER BY logical_date"
    ),
    "revenue": (
        "SELECT order_date, gross_order_value, net_revenue FROM marts.fct_daily_revenue "
        "WHERE order_date BETWEEN %(start)s AND %(end)s ORDER BY order_date"
    ),
    "quality": (
        "SELECT logical_date, outcome, COUNT(*) AS checks FROM audit.quality_results "
        "WHERE logical_date BETWEEN %(start)s AND %(end)s GROUP BY 1, 2 ORDER BY 1"
    ),
    "attempts": (
        "SELECT logical_date, status, COUNT(*) AS attempts FROM audit.task_runs "
        "WHERE logical_date BETWEEN %(start)s AND %(end)s GROUP BY 1, 2 ORDER BY 1"
    ),
}

COLORS = {"pass": "#16a34a", "warn": "#f59e0b", "fail": "#dc2626", "skipped": "#94a3b8"}


def load(conn: Any, start: date, end: date) -> dict[str, list[tuple]]:
    params = {"start": start, "end": end}
    return {name: conn.execute(sql, params).fetchall() for name, sql in QUERIES.items()}


def render(data: dict[str, list[tuple]], output: Path, subtitle: str) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    if not data["records"]:
        raise ValueError("no ingestion batches found for the requested dates")
    fig, axes = plt.subplots(2, 2, figsize=(12, 7.5), constrained_layout=True)
    fig.suptitle(f"DataOps Sentinel - {subtitle}", fontsize=12, fontweight="bold")

    ax = axes[0][0]
    days = [str(row[0])[5:] for row in data["records"]]
    accepted = [int(row[1]) for row in data["records"]]
    rejected = [int(row[2]) for row in data["records"]]
    ax.bar(days, accepted, color="#2563eb", label="accepted")
    ax.bar(days, rejected, bottom=accepted, color="#dc2626", label="rejected")
    ax.set_title("Records per logical date (all datasets)")
    ax.legend(frameon=False)

    ax = axes[0][1]
    rev_days = [str(row[0])[5:] for row in data["revenue"]]
    gross = [float(row[1]) for row in data["revenue"]]
    net = [float(row[2]) for row in data["revenue"]]
    ax.plot(rev_days, gross, marker="o", label="gross order value")
    ax.plot(rev_days, net, marker="o", label="net revenue")
    ax.set_title("marts.fct_daily_revenue (EUR, synthetic)")
    ax.legend(frameon=False)

    ax = axes[1][0]
    by_day: dict[str, dict[str, int]] = defaultdict(dict)
    for logical_date, outcome, count in data["quality"]:
        by_day[str(logical_date)[5:]][outcome] = int(count)
    labels = sorted(by_day)
    bottom = [0] * len(labels)
    for outcome in ("pass", "warn", "fail", "skipped"):
        values = [by_day[label].get(outcome, 0) for label in labels]
        ax.bar(labels, values, bottom=bottom, color=COLORS[outcome], label=outcome)
        bottom = [b + v for b, v in zip(bottom, values, strict=True)]
    ax.set_title("Latest quality-check outcomes per date")
    ax.set_ylim(0, max(bottom, default=1) * 1.3)
    ax.legend(frameon=False, ncol=4, loc="upper center")

    ax = axes[1][1]
    attempts: dict[str, dict[str, int]] = defaultdict(dict)
    for logical_date, status, count in data["attempts"]:
        attempts[str(logical_date)[5:]][status] = int(count)
    labels = sorted(attempts)
    ok = [attempts[label].get("success", 0) for label in labels]
    failed = [attempts[label].get("failed", 0) for label in labels]
    ax.bar(labels, ok, color="#16a34a", label="successful attempts")
    ax.bar(labels, failed, bottom=ok, color="#dc2626", label="failed attempts")
    ax.set_title("Task attempts per date, including re-runs (audit.task_runs)")
    ax.set_ylim(0, max((a + b for a, b in zip(ok, failed, strict=True)), default=1) * 1.3)
    ax.legend(frameon=False, ncol=2, loc="upper center")

    for row in axes:
        for axis in row:
            axis.spines[["top", "right"]].set_visible(False)
            axis.tick_params(axis="x", rotation=30)
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, dpi=120)


def main(argv: list[str] | None = None) -> int:
    from dataops_sentinel.config import Settings
    from dataops_sentinel.database.connection import open_connection
    from dataops_sentinel.logging_config import configure_logging

    settings = Settings.from_env()
    configure_logging(settings.log_level)
    parser = argparse.ArgumentParser(description="Render the pipeline run summary chart.")
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--output", type=Path, default=Path("docs/images/pipeline_run_summary.png"))
    parser.add_argument("--subtitle", default="local pipeline run")
    args = parser.parse_args(argv)
    with open_connection(settings.database) as conn:
        data = load(conn, args.start, args.end)
    try:
        render(data, args.output, args.subtitle)
    except ValueError as exc:
        logger.error("Nothing to render", extra={"error": str(exc)})
        return 1
    logger.info("Chart written", extra={"path": str(args.output)})
    return 0


if __name__ == "__main__":
    sys.exit(main())
