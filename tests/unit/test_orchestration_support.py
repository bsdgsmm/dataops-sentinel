from __future__ import annotations

import subprocess
from datetime import UTC, date, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from dataops_sentinel.config import ConfigError, Settings
from dataops_sentinel.errors import DataQualityError, PermanentPipelineError, TransientPipelineError
from dataops_sentinel.orchestration import dbt_runner
from dataops_sentinel.orchestration.faults import FaultInjector, parse_rules
from dataops_sentinel.reporting.report import ReportData, render_markdown

DAY = date(2026, 1, 5)


def test_fault_rules_parse_and_validate() -> None:
    rules = parse_rules("ingest_orders@2026-01-05:transient_once, dbt_build@2026-01-06:always")
    assert [(r.task, r.logical_date, r.mode) for r in rules] == [
        ("ingest_orders", DAY, "transient_once"),
        ("dbt_build", date(2026, 1, 6), "always"),
    ]
    for bad in ("ingest_orders:transient_once", "x@2026-01-05:sometimes", "x@yesterday:always"):
        with pytest.raises(ValueError):
            parse_rules(bad)


def test_transient_once_fires_exactly_once(tmp_path) -> None:
    injector = FaultInjector("ingest_orders@2026-01-05:transient_once", tmp_path)
    injector.maybe_fail("ingest_orders", date(2026, 1, 4))
    injector.maybe_fail("ingest_payments", DAY)
    with pytest.raises(TransientPipelineError):
        injector.maybe_fail("ingest_orders", DAY)
    injector.maybe_fail("ingest_orders", DAY)


def test_always_mode_never_recovers(tmp_path) -> None:
    injector = FaultInjector("dbt_build@2026-01-05:always", tmp_path)
    for _ in range(3):
        with pytest.raises(TransientPipelineError):
            injector.maybe_fail("dbt_build", DAY)


def test_no_rules_means_no_faults(tmp_path) -> None:
    FaultInjector("", tmp_path).maybe_fail("ingest_orders", DAY)


def settings() -> Settings:
    return Settings.from_env(
        {"SENTINEL_DB_PASSWORD": "pw", "SENTINEL_DBT_EXECUTABLE": "/opt/dbt/bin/dbt"}
    )


def test_dbt_command_and_environment() -> None:
    s = settings()
    command = dbt_runner.build_command(s)
    assert command[:2] == ["/opt/dbt/bin/dbt", "build"]
    assert dbt_runner.dbt_environment(s)["SENTINEL_DB_PASSWORD"] == "pw"


@pytest.mark.parametrize(
    ("code", "output", "error"),
    [
        (0, "Completed successfully", None),
        (
            1,
            "Failure in test unique_stg_orders_order_id (models/staging/_staging.yml)",
            DataQualityError,
        ),
        (2, "could not connect to server: Connection refused", TransientPipelineError),
        (1, "Compilation Error in model stg_orders", PermanentPipelineError),
    ],
)
def test_dbt_exit_status_mapping(code, output, error) -> None:
    def runner(*_args, **_kwargs):
        return SimpleNamespace(returncode=code, stdout=output, stderr="")

    if error is None:
        assert dbt_runner.run_dbt(settings(), DAY, runner=runner) == output
    else:
        with pytest.raises(error):
            dbt_runner.run_dbt(settings(), DAY, runner=runner)


def test_dbt_missing_executable_and_timeout() -> None:
    def missing(*_a, **_k):
        raise FileNotFoundError

    def slow(*_a, **_k):
        raise subprocess.TimeoutExpired("dbt", 1)

    with pytest.raises(PermanentPipelineError):
        dbt_runner.run_dbt(settings(), DAY, runner=missing)
    with pytest.raises(TransientPipelineError):
        dbt_runner.run_dbt(settings(), DAY, runner=slow)


def test_settings_defaults_and_validation() -> None:
    s = Settings.from_env({})
    assert s.database.port == 5432 and s.thresholds.reject_rate_fail == 0.05
    assert "pw" not in repr(Settings.from_env({"SENTINEL_DB_PASSWORD": "pw"}))
    with pytest.raises(ConfigError):
        Settings.from_env({"SENTINEL_DB_PORT": "abc"})
    with pytest.raises(ConfigError):
        Settings.from_env({"SENTINEL_REJECT_RATE_WARN": "0.5", "SENTINEL_REJECT_RATE_FAIL": "0.1"})


def test_report_renders_runs_quality_errors_and_repeated_attempts() -> None:
    data = ReportData(
        runs=[
            {
                "run_id": "r",
                "logical_date": DAY,
                "trigger": "airflow",
                "status": "success",
                "duration_seconds": Decimal("12.5"),
                "error": None,
                "started_at": None,
                "finished_at": None,
            }
        ],
        batches=[
            {
                "logical_date": DAY,
                "dataset": "orders",
                "records_read": 100,
                "records_accepted": 97,
                "records_rejected": 3,
                "try_number": 2,
            }
        ],
        quality=[
            {
                "logical_date": DAY,
                "stage": "raw",
                "check_name": "reject_rate_orders",
                "severity": "blocking",
                "outcome": "warn",
                "observed": Decimal("0.03"),
                "details": "3/100",
            },
            {
                "logical_date": DAY,
                "stage": "raw",
                "check_name": "orders_product_fk",
                "severity": "blocking",
                "outcome": "pass",
                "observed": Decimal("0"),
                "details": "0/97",
            },
        ],
        tasks=[
            {
                "logical_date": DAY,
                "run_id": "r",
                "task_name": "ingest_orders",
                "try_number": 1,
                "status": "failed",
                "duration_seconds": 1,
                "error_type": "TransientPipelineError",
                "error": "blip",
            },
            {
                "logical_date": DAY,
                "run_id": "r",
                "task_name": "ingest_orders",
                "try_number": 2,
                "status": "success",
                "duration_seconds": 1,
                "error_type": None,
                "error": None,
            },
        ],
    )
    text = render_markdown(data, DAY, DAY, datetime(2026, 1, 6, 3, 0, tzinfo=UTC))
    assert "| 2026-01-05 | r | airflow | success | 12.50 | - |" in text
    assert "| 2026-01-05 | orders | 100 | 97 | 3 | 3.00 | 2 |" in text
    assert "Outcomes: pass=1, warn=1." in text
    assert "reject_rate_orders" in text and "orders_product_fk" not in text
    assert "| 2026-01-05 | ingest_orders | 1 | TransientPipelineError | blip |" in text
    assert "| 2026-01-05 | ingest_orders | 2 | success |" in text


def test_report_handles_empty_audit_tables() -> None:
    text = render_markdown(ReportData([], [], [], []), DAY, DAY, datetime(2026, 1, 6, tzinfo=UTC))
    assert text.count("_No records._") == 5
