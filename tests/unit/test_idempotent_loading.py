from __future__ import annotations

from dataclasses import replace
from datetime import date

import pytest

from dataops_sentinel.config import Settings
from dataops_sentinel.database.repository import replace_partition
from dataops_sentinel.errors import SourceFileError, TransientPipelineError
from dataops_sentinel.ingestion.reader import read_source_file
from dataops_sentinel.ingestion.schemas import ORDERS
from dataops_sentinel.orchestration import tasks
from dataops_sentinel.validation.records import validate_file
from tests.conftest import FIXTURE_DATE
from tests.fakes import FakeConnection


def validated(landing_dir):
    return validate_file(read_source_file(landing_dir, FIXTURE_DATE, ORDERS))


def orders_for(conn: FakeConnection, day: date) -> list[str]:
    return sorted(
        row["order_id"] for row in conn.tables["raw.orders"] if row["logical_date"] == day
    )


def test_loading_the_same_date_twice_does_not_duplicate_rows(landing_dir) -> None:
    conn = FakeConnection()
    result = validated(landing_dir)
    first = replace_partition(conn, ORDERS, result, run_id="r1", try_number=1, source_path="x")
    second = replace_partition(conn, ORDERS, result, run_id="r2", try_number=2, source_path="x")
    assert orders_for(conn, FIXTURE_DATE) == ["O00000001", "O00000009"]
    assert first.rows_replaced == 0
    assert second.rows_replaced == 2
    assert len(conn.tables["raw.rejected_records"]) == 8
    assert len(conn.tables["audit.ingestion_batches"]) == 1
    assert conn.tables["audit.ingestion_batches"][0]["run_id"] == "r2"


def test_other_dates_are_not_touched(landing_dir) -> None:
    conn = FakeConnection()
    other = date(2026, 1, 1)
    conn.tables["raw.orders"].append({"order_id": "O00000099", "logical_date": other})
    replace_partition(
        conn, ORDERS, validated(landing_dir), run_id="r", try_number=1, source_path="x"
    )
    assert orders_for(conn, other) == ["O00000099"]


def test_failure_before_commit_rolls_back_the_whole_partition(landing_dir) -> None:
    conn = FakeConnection()
    replace_partition(
        conn, ORDERS, validated(landing_dir), run_id="good", try_number=1, source_path="x"
    )

    def explode() -> None:
        raise TransientPipelineError("network blip")

    smaller = replace(validated(landing_dir), accepted=validated(landing_dir).accepted[:1])
    with pytest.raises(TransientPipelineError):
        replace_partition(
            conn,
            ORDERS,
            smaller,
            run_id="bad",
            try_number=2,
            source_path="x",
            before_commit=explode,
        )
    assert conn.rollbacks == 1
    assert orders_for(conn, FIXTURE_DATE) == ["O00000001", "O00000009"]
    assert {row["run_id"] for row in conn.tables["raw.orders"]} == {"good"}


def test_database_error_mid_load_leaves_previous_state(landing_dir) -> None:
    conn = FakeConnection()
    replace_partition(
        conn, ORDERS, validated(landing_dir), run_id="good", try_number=1, source_path="x"
    )
    conn.fail_on = "INSERT INTO raw.rejected_records"
    with pytest.raises(ConnectionError):
        replace_partition(
            conn, ORDERS, validated(landing_dir), run_id="bad", try_number=2, source_path="x"
        )
    assert {row["run_id"] for row in conn.tables["raw.orders"]} == {"good"}
    assert len(conn.tables["raw.rejected_records"]) == 8


def settings_for(landing_dir, tmp_path, fault: str = "") -> Settings:
    return Settings.from_env(
        {
            "SENTINEL_LANDING_DIR": str(landing_dir),
            "SENTINEL_STATE_DIR": str(tmp_path / "state"),
            "SENTINEL_FAULT_INJECTION": fault,
        }
    )


def test_injected_transient_failure_then_retry_succeeds(landing_dir, tmp_path) -> None:
    conn = FakeConnection()
    settings = settings_for(landing_dir, tmp_path, f"ingest_orders@{FIXTURE_DATE}:transient_once")
    with pytest.raises(TransientPipelineError):
        tasks.ingest_dataset(
            settings, "orders", FIXTURE_DATE, "run", 1, connection_factory=lambda: conn
        )
    assert orders_for(conn, FIXTURE_DATE) == []
    summary = tasks.ingest_dataset(
        settings, "orders", FIXTURE_DATE, "run", 2, connection_factory=lambda: conn
    )
    assert summary["records_accepted"] == 2
    assert orders_for(conn, FIXTURE_DATE) == ["O00000001", "O00000009"]
    assert conn.closed


def test_task_attempts_are_recorded_including_failures(landing_dir, tmp_path) -> None:
    conn = FakeConnection()
    settings = settings_for(landing_dir, tmp_path, f"ingest_orders@{FIXTURE_DATE}:transient_once")
    with pytest.raises(TransientPipelineError):
        tasks.ingest_dataset(
            settings, "orders", FIXTURE_DATE, "run", 1, connection_factory=lambda: conn
        )
    tasks.ingest_dataset(
        settings, "orders", FIXTURE_DATE, "run", 2, connection_factory=lambda: conn
    )
    starts = [s for s in conn.statements if s.startswith("INSERT INTO audit.task_runs")]
    finishes = [s for s in conn.statements if s.startswith("UPDATE audit.task_runs")]
    assert len(starts) == 2 and len(finishes) == 2


def test_contract_violation_is_permanent_and_writes_nothing(landing_dir, tmp_path) -> None:
    conn = FakeConnection()
    settings = settings_for(landing_dir, tmp_path)
    with pytest.raises(SourceFileError):
        tasks.ingest_dataset(
            settings, "orders", date(2026, 1, 3), "run", 1, connection_factory=lambda: conn
        )
    assert conn.tables["raw.orders"] == []
