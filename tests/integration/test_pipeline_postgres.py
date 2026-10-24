from __future__ import annotations

import shutil
from dataclasses import replace
from datetime import date

import pytest

from dataops_sentinel.errors import DataQualityError, SourceFileError, TransientPipelineError
from dataops_sentinel.ingestion.schemas import LOAD_ORDER
from dataops_sentinel.orchestration import tasks
from dataops_sentinel.synthetic.generator import generate_day, write_day

D1, D2, D3 = date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)


def land(settings, day, scenario="default"):
    write_day(generate_day(7, day, scenario), settings.landing_dir, 7, scenario)


def ingest_all(settings, day, run_id="it", try_number=1):
    return {ds: tasks.ingest_dataset(settings, ds, day, run_id, try_number) for ds in LOAD_ORDER}


def scalar(conn, sql, *params):
    return conn.execute(sql, params).fetchone()[0]


def test_rerunning_a_date_replaces_instead_of_appending(settings, conn) -> None:
    land(settings, D1)
    first = ingest_all(settings, D1)
    counts = scalar(conn, "SELECT COUNT(*) FROM raw.orders WHERE logical_date = %s", D1)
    second = ingest_all(settings, D1, try_number=2)
    assert scalar(conn, "SELECT COUNT(*) FROM raw.orders WHERE logical_date = %s", D1) == counts
    assert counts == first["orders"]["records_accepted"]
    assert second["orders"]["rows_replaced"] == counts
    assert scalar(conn, "SELECT COUNT(*) FROM audit.ingestion_batches") == 4


def test_failure_before_commit_keeps_previous_partition(settings, conn) -> None:
    land(settings, D1)
    ingest_all(settings, D1, run_id="good")
    faulty = replace(settings, fault_injection=f"ingest_orders@{D1}:transient_once")
    with pytest.raises(TransientPipelineError):
        tasks.ingest_dataset(faulty, "orders", D1, "retry", 1)
    assert scalar(conn, "SELECT COUNT(DISTINCT run_id) FROM raw.orders") == 1
    assert scalar(conn, "SELECT MIN(run_id) FROM raw.orders") == "good"
    tasks.ingest_dataset(faulty, "orders", D1, "retry", 2)
    assert scalar(conn, "SELECT MIN(run_id) FROM raw.orders") == "retry"
    attempts = conn.execute(
        "SELECT try_number, status FROM audit.task_runs WHERE run_id = 'retry' ORDER BY 1"
    ).fetchall()
    assert attempts == [(1, "failed"), (2, "success")]


def test_raw_quality_gate_passes_for_default_data(settings, conn) -> None:
    for day in (D1, D2):
        land(settings, day)
        ingest_all(settings, day)
        results = tasks.check_raw_quality(settings, day, "it")
    outcomes = {r.name: r.outcome.value for r in results}
    assert outcomes["duplicate_business_keys"] == "pass"
    assert outcomes["orders_row_count_change"] in {"pass", "warn"}
    stored = "SELECT COUNT(*) FROM audit.quality_results WHERE logical_date = %s"
    assert scalar(conn, stored, D2) == 13


def test_dirty_data_is_blocked_and_recorded(settings, conn) -> None:
    land(settings, D1)
    ingest_all(settings, D1)
    land(settings, D2, "dirty")
    ingest_all(settings, D2)
    with pytest.raises(DataQualityError) as info:
        tasks.check_raw_quality(settings, D2, "it")
    assert "reject_rate_orders" in info.value.failed_checks
    stored = scalar(
        conn,
        "SELECT outcome FROM audit.quality_results WHERE logical_date = %s "
        "AND check_name = 'reject_rate_orders'",
        D2,
    )
    assert stored == "fail"
    assert scalar(conn, "SELECT COUNT(*) FROM raw.rejected_records WHERE logical_date = %s", D2) > 0


def test_contract_violation_does_not_touch_existing_partition(settings, conn) -> None:
    land(settings, D1)
    ingest_all(settings, D1)
    before = scalar(conn, "SELECT COUNT(*) FROM raw.orders")
    shutil.rmtree(settings.landing_dir / D1.isoformat())
    land(settings, D1, "missing_column")
    with pytest.raises(SourceFileError):
        tasks.ingest_dataset(settings, "orders", D1, "it", 2)
    assert scalar(conn, "SELECT COUNT(*) FROM raw.orders") == before


def test_backfill_out_of_order_produces_same_state(settings, conn) -> None:
    for day in (D1, D2, D3):
        land(settings, day)
    for day in (D3, D1, D2):
        ingest_all(settings, day)
    snapshot = conn.execute(
        "SELECT logical_date, COUNT(*), SUM(order_total) FROM raw.orders GROUP BY 1 ORDER BY 1"
    ).fetchall()
    ingest_all(settings, D2, try_number=2)
    again = conn.execute(
        "SELECT logical_date, COUNT(*), SUM(order_total) FROM raw.orders GROUP BY 1 ORDER BY 1"
    ).fetchall()
    assert snapshot == again
    assert [row[0] for row in snapshot] == [D1, D2, D3]


@pytest.mark.skipif(shutil.which("dbt") is None, reason="dbt executable not installed")
def test_dbt_build_and_mart_reconciliation(settings, conn) -> None:
    for day in (D1, D2):
        land(settings, day)
        ingest_all(settings, day)
        tasks.check_raw_quality(settings, day, "it")
    tasks.transform(settings, D2, "it")
    results = tasks.check_mart_quality(settings, D2, "it")
    assert all(r.outcome.value == "pass" for r in results)
    assert scalar(conn, "SELECT COUNT(*) FROM marts.fct_daily_revenue") == 2
