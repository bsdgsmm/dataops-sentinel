from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from dataops_sentinel.config import QualityThresholds
from dataops_sentinel.errors import DataQualityError
from dataops_sentinel.orchestration import tasks
from dataops_sentinel.validation import quality
from dataops_sentinel.validation.quality import Outcome, Severity
from tests.fakes import FakeConnection

T = QualityThresholds()


@pytest.mark.parametrize(
    ("read", "rejected", "outcome"),
    [
        (100, 0, Outcome.PASS),
        (100, 1, Outcome.PASS),
        (100, 3, Outcome.WARN),
        (100, 6, Outcome.FAIL),
        (0, 0, Outcome.SKIPPED),
    ],
)
def test_reject_rate_thresholds(read: int, rejected: int, outcome: Outcome) -> None:
    result = quality.check_reject_rate("orders", read, rejected, T)
    assert result.outcome is outcome
    assert result.severity is Severity.BLOCKING


def test_only_blocking_failures_stop_the_pipeline() -> None:
    results = [
        quality.check_reject_rate("orders", 100, 3, T),
        quality.check_zero("orders_already_loaded_on_earlier_date", 4, Severity.WARNING, "x"),
        quality.check_orphans("orders_customer_fk", 1, 100, "orders", T),
    ]
    assert [r.outcome for r in results] == [Outcome.WARN, Outcome.WARN, Outcome.WARN]
    assert quality.blocking_failures(results) == []
    results.append(quality.check_orphans("payments_order_fk", 10, 100, "payments", T))
    assert [r.name for r in quality.blocking_failures(results)] == ["payments_order_fk"]


def test_duplicate_business_keys_are_blocking() -> None:
    result = quality.check_zero("duplicate_business_keys", 2, Severity.BLOCKING, "dups")
    assert result.is_blocking_failure


def test_row_count_change() -> None:
    assert quality.check_row_count_change(0, [100], T).outcome is Outcome.FAIL
    assert quality.check_row_count_change(120, [], T).outcome is Outcome.SKIPPED
    assert quality.check_row_count_change(120, [100, 110], T).outcome is Outcome.PASS
    spike = quality.check_row_count_change(400, [100, 110], T)
    assert spike.outcome is Outcome.WARN
    assert spike.observed == Decimal("2.8095")


@pytest.mark.parametrize(
    ("expected", "actual", "outcome"),
    [
        ("1000.00", "1000.00", Outcome.PASS),
        ("1000.00", "995.00", Outcome.PASS),
        ("1000.00", "980.00", Outcome.WARN),
        ("1000.00", "850.00", Outcome.FAIL),
        ("0", "0", Outcome.PASS),
        ("0", "5.00", Outcome.FAIL),
    ],
)
def test_revenue_reconciliation(expected: str, actual: str, outcome: Outcome) -> None:
    result = quality.check_revenue_reconciliation(
        "r", Decimal(expected), Decimal(actual), T, "label"
    )
    assert result.outcome is outcome


def test_rejections_by_reason_summarises_counts() -> None:
    result = quality.check_rejections_by_reason(
        {"orders.invalid_status": 2, "orders.negative_amount": 1}
    )
    assert result.outcome is Outcome.WARN
    assert result.details == "orders.invalid_status=2, orders.negative_amount=1"
    assert quality.check_rejections_by_reason({}).outcome is Outcome.PASS


def raw_metrics_connection(**overrides) -> FakeConnection:
    conn = FakeConnection()
    q = quality.RAW_METRIC_QUERIES
    values = {
        "batches": [
            ("customers", 10, 0, {}),
            ("products", 0, 0, {}),
            ("orders", 100, 2, {"invalid_status": 2}),
            ("payments", 95, 0, {}),
        ],
        "duplicate_keys_in_partition": [(0,)],
        "orders_seen_on_earlier_dates": [(0,)],
        "orders_total": [(98,)],
        "orders_orphan_customer": [(0,)],
        "orders_orphan_product": [(0,)],
        "payments_total": [(95,)],
        "payments_orphan_order": [(0,)],
        "order_count_history": [(date(2026, 1, 1), 90), (date(2026, 1, 2), 110)],
        "order_value_vs_payments": [(Decimal("5000.00"), Decimal("5000.00"))],
    }
    values.update(overrides)
    for key, rows in values.items():
        conn.query_results[q[key]] = rows
    return conn


def test_run_raw_checks_on_healthy_metrics() -> None:
    results = quality.run_raw_checks(raw_metrics_connection(), date(2026, 1, 3), T)
    by_name = {r.name: r.outcome for r in results}
    assert by_name["reject_rate_orders"] is Outcome.WARN
    assert by_name["reject_rate_products"] is Outcome.SKIPPED
    assert by_name["all_datasets_loaded"] is Outcome.PASS
    assert by_name["order_value_vs_captured_payments"] is Outcome.PASS
    assert quality.blocking_failures(results) == []


def test_missing_dataset_and_broken_foreign_keys_block() -> None:
    conn = raw_metrics_connection(
        batches=[("customers", 10, 0, {}), ("orders", 100, 0, {})],
        payments_orphan_order=[(20,)],
    )
    failed = {
        r.name for r in quality.blocking_failures(quality.run_raw_checks(conn, date(2026, 1, 3), T))
    }
    assert failed == {"all_datasets_loaded", "payments_order_fk"}


def test_quality_task_persists_results_then_raises(tmp_path) -> None:
    from dataops_sentinel.config import Settings

    conn = raw_metrics_connection(orders_orphan_customer=[(30,)])
    settings = Settings.from_env({"SENTINEL_STATE_DIR": str(tmp_path)})
    with pytest.raises(DataQualityError) as info:
        tasks.check_raw_quality(
            settings, date(2026, 1, 3), "run", 1, connection_factory=lambda: conn
        )
    assert info.value.failed_checks == ["orders_customer_fk"]
    saved = conn.tables["audit.quality_results"]
    assert len(saved) == 13
    assert {row["outcome"] for row in saved} >= {"fail", "pass"}


def test_mart_checks_require_exact_revenue_match() -> None:
    conn = FakeConnection()
    q = quality.MART_METRIC_QUERIES
    conn.query_results[q["mart_daily_row_present"]] = [(1,)]
    conn.query_results[q["net_revenue_raw_vs_mart"]] = [(Decimal("100.00"), Decimal("99.99"))]
    results = quality.run_mart_checks(conn, date(2026, 1, 3), T)
    assert [r.name for r in quality.blocking_failures(results)] == ["net_revenue_raw_vs_mart"]
