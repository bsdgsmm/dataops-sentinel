"""Run the daily pipeline without Airflow, for local development, CI and demos.

The task order matches the Airflow DAG. Retries are Airflow's job; the CLI
runs each task once and stops at the first failure, so a re-run of the same
command demonstrates recovery.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date

from dataops_sentinel.config import Settings
from dataops_sentinel.errors import SentinelError
from dataops_sentinel.ingestion.schemas import LOAD_ORDER
from dataops_sentinel.logging_config import configure_logging
from dataops_sentinel.orchestration import tasks
from dataops_sentinel.synthetic.generator import SCENARIOS, date_range, generate_day, write_day

logger = logging.getLogger("dataops_sentinel.cli")


def run_date(
    settings: Settings,
    logical_date: date,
    *,
    try_number: int,
    skip_dbt: bool,
) -> bool:
    run_id = f"cli__{logical_date.isoformat()}"
    tasks.start_run(settings, run_id, logical_date, "cli")
    try:
        for dataset in LOAD_ORDER:
            tasks.ingest_dataset(settings, dataset, logical_date, run_id, try_number)
        tasks.check_raw_quality(settings, logical_date, run_id, try_number)
        if not skip_dbt:
            tasks.transform(settings, logical_date, run_id, try_number)
            tasks.check_mart_quality(settings, logical_date, run_id, try_number)
    except SentinelError as exc:
        tasks.finish_run(settings, run_id, "failed", f"{type(exc).__name__}: {exc}")
        logger.error(
            "Pipeline failed",
            extra={
                "logical_date": str(logical_date),
                "error_type": type(exc).__name__,
                "error": str(exc),
            },
        )
        return False
    tasks.finish_run(settings, run_id, "success")
    logger.info("Pipeline succeeded", extra={"logical_date": str(logical_date)})
    return True


def main(argv: list[str] | None = None) -> int:
    settings = Settings.from_env()
    configure_logging(settings.log_level)
    parser = argparse.ArgumentParser(description="Run the DataOps Sentinel pipeline locally.")
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat)
    parser.add_argument(
        "--generate", action="store_true", help="write synthetic landing files first"
    )
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--scenario", choices=SCENARIOS, default="default")
    parser.add_argument("--try-number", type=int, default=1)
    parser.add_argument("--skip-dbt", action="store_true")
    parser.add_argument("--init-db", action="store_true", help="apply sql/schema.sql first")
    args = parser.parse_args(argv)

    if args.init_db:
        tasks.ensure_schema(settings)

    failed = []
    for logical_date in date_range(args.start, args.end or args.start):
        if args.generate:
            write_day(
                generate_day(args.seed, logical_date, args.scenario),
                settings.landing_dir,
                args.seed,
                args.scenario,
            )
        if not run_date(settings, logical_date, try_number=args.try_number, skip_dbt=args.skip_dbt):
            failed.append(logical_date.isoformat())
            break
    if failed:
        logger.error("Stopped at first failed date", extra={"failed": failed})
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
