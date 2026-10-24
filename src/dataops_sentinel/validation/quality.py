"""Dataset-level data-quality checks for one logical date.

Each check produces a :class:`CheckResult` with a severity:

* ``blocking`` - a ``fail`` outcome stops the pipeline for this date;
* ``warning`` - a ``warn`` or ``fail`` outcome is reported but does not stop it.

The SQL in :data:`RAW_METRIC_QUERIES` collects the numbers; the evaluation
functions turn numbers into outcomes and are tested without a database.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from enum import StrEnum
from typing import Any

from dataops_sentinel.config import QualityThresholds


class Severity(StrEnum):
    BLOCKING = "blocking"
    WARNING = "warning"


class Outcome(StrEnum):
    PASS = "pass"
    WARN = "warn"
    FAIL = "fail"
    SKIPPED = "skipped"


@dataclass(frozen=True)
class CheckResult:
    name: str
    severity: Severity
    outcome: Outcome
    observed: Decimal | None
    threshold: Decimal | None
    details: str

    @property
    def is_blocking_failure(self) -> bool:
        return self.severity is Severity.BLOCKING and self.outcome is Outcome.FAIL


def _dec(value: Any) -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value)).quantize(Decimal("0.0001"))


def rate(numerator: float, denominator: float) -> float:
    return numerator / denominator if denominator else 0.0


def check_reject_rate(
    dataset: str, read: int, rejected: int, thresholds: QualityThresholds
) -> CheckResult:
    observed = rate(rejected, read)
    if read == 0:
        outcome = Outcome.SKIPPED
        details = f"{dataset}: source file had no records"
    elif observed > thresholds.reject_rate_fail:
        outcome = Outcome.FAIL
        details = f"{dataset}: {rejected}/{read} records rejected"
    elif observed > thresholds.reject_rate_warn:
        outcome = Outcome.WARN
        details = f"{dataset}: {rejected}/{read} records rejected"
    else:
        outcome = Outcome.PASS
        details = f"{dataset}: {rejected}/{read} records rejected"
    return CheckResult(
        f"reject_rate_{dataset}",
        Severity.BLOCKING,
        outcome,
        _dec(observed),
        _dec(thresholds.reject_rate_fail),
        details,
    )


def check_rejections_by_reason(reasons: Mapping[str, int]) -> CheckResult:
    total = sum(reasons.values())
    details = ", ".join(f"{reason}={count}" for reason, count in sorted(reasons.items()))
    return CheckResult(
        "rejected_records_by_reason",
        Severity.WARNING,
        Outcome.WARN if total else Outcome.PASS,
        _dec(total),
        _dec(0),
        details or "no rejected records",
    )


def check_zero(name: str, observed: int, severity: Severity, what: str) -> CheckResult:
    return CheckResult(
        name,
        severity,
        Outcome.PASS
        if observed == 0
        else (Outcome.FAIL if severity is Severity.BLOCKING else Outcome.WARN),
        _dec(observed),
        _dec(0),
        f"{observed} {what}",
    )


def check_orphans(
    name: str, orphans: int, total: int, what: str, thresholds: QualityThresholds
) -> CheckResult:
    observed = rate(orphans, total)
    if observed > thresholds.orphan_rate_fail:
        outcome = Outcome.FAIL
    elif observed > thresholds.orphan_rate_warn:
        outcome = Outcome.WARN
    else:
        outcome = Outcome.PASS
    return CheckResult(
        name,
        Severity.BLOCKING,
        outcome,
        _dec(observed),
        _dec(thresholds.orphan_rate_fail),
        f"{orphans}/{total} {what}",
    )


def check_row_count_change(
    current: int, history: Sequence[int], thresholds: QualityThresholds
) -> CheckResult:
    name = "orders_row_count_change"
    if current == 0:
        return CheckResult(name, Severity.BLOCKING, Outcome.FAIL, _dec(0), None, "no orders loaded")
    if not history:
        return CheckResult(
            name, Severity.WARNING, Outcome.SKIPPED, None, None, "no earlier dates to compare"
        )
    baseline = sum(history) / len(history)
    change = (current - baseline) / baseline if baseline else 0.0
    outcome = Outcome.WARN if abs(change) > thresholds.row_count_change_warn else Outcome.PASS
    return CheckResult(
        name,
        Severity.WARNING,
        outcome,
        _dec(change),
        _dec(thresholds.row_count_change_warn),
        f"{current} orders vs trailing average {baseline:.1f} over {len(history)} date(s)",
    )


def check_revenue_reconciliation(
    name: str,
    expected: Decimal,
    actual: Decimal,
    thresholds: QualityThresholds,
    label: str,
) -> CheckResult:
    difference = abs(expected - actual)
    observed = float(difference / expected) if expected else float(difference > 0)
    if observed > thresholds.revenue_diff_fail:
        outcome = Outcome.FAIL
    elif observed > thresholds.revenue_diff_warn:
        outcome = Outcome.WARN
    else:
        outcome = Outcome.PASS
    return CheckResult(
        name,
        Severity.BLOCKING,
        outcome,
        _dec(observed),
        _dec(thresholds.revenue_diff_fail),
        f"{label}: expected {expected}, actual {actual}, difference {difference}",
    )


RAW_METRIC_QUERIES: dict[str, str] = {
    "batches": (
        "SELECT dataset, records_read, records_rejected, reject_reasons "
        "FROM audit.ingestion_batches WHERE logical_date = %(d)s"
    ),
    "duplicate_keys_in_partition": (
        "SELECT (SELECT COUNT(*) FROM (SELECT order_id FROM raw.orders WHERE logical_date = %(d)s "
        "GROUP BY order_id HAVING COUNT(*) > 1) o) + "
        "(SELECT COUNT(*) FROM (SELECT payment_id FROM raw.payments WHERE logical_date = %(d)s "
        "GROUP BY payment_id HAVING COUNT(*) > 1) p) AS value"
    ),
    "orders_seen_on_earlier_dates": (
        "SELECT COUNT(*) AS value FROM raw.orders o WHERE o.logical_date = %(d)s AND EXISTS "
        "(SELECT 1 FROM raw.orders e WHERE e.order_id = o.order_id AND e.logical_date < %(d)s)"
    ),
    "orders_total": "SELECT COUNT(*) AS value FROM raw.orders WHERE logical_date = %(d)s",
    "orders_orphan_customer": (
        "SELECT COUNT(*) AS value FROM raw.orders o WHERE o.logical_date = %(d)s AND NOT EXISTS "
        "(SELECT 1 FROM raw.customers c WHERE c.customer_id = o.customer_id "
        "AND c.logical_date <= %(d)s)"
    ),
    "orders_orphan_product": (
        "SELECT COUNT(*) AS value FROM raw.orders o WHERE o.logical_date = %(d)s AND NOT EXISTS "
        "(SELECT 1 FROM raw.products p WHERE p.product_id = o.product_id "
        "AND p.logical_date <= %(d)s)"
    ),
    "payments_total": "SELECT COUNT(*) AS value FROM raw.payments WHERE logical_date = %(d)s",
    "payments_orphan_order": (
        "SELECT COUNT(*) AS value FROM raw.payments p WHERE p.logical_date = %(d)s AND NOT EXISTS "
        "(SELECT 1 FROM raw.orders o WHERE o.order_id = p.order_id AND o.logical_date <= %(d)s)"
    ),
    "order_count_history": (
        "SELECT logical_date, COUNT(*) AS value FROM raw.orders "
        "WHERE logical_date < %(d)s AND logical_date >= %(d)s::date - 7 "
        "GROUP BY logical_date ORDER BY logical_date"
    ),
    "order_value_vs_payments": (
        "WITH day_orders AS (SELECT order_id, order_total FROM raw.orders "
        "WHERE logical_date = %(d)s AND status <> 'cancelled'), "
        "captured AS (SELECT p.order_id, SUM(p.amount) AS amount FROM raw.payments p "
        "JOIN day_orders o ON o.order_id = p.order_id "
        "WHERE p.status = 'captured' AND p.logical_date >= %(d)s GROUP BY p.order_id) "
        "SELECT COALESCE((SELECT SUM(order_total) FROM day_orders), 0) AS expected, "
        "COALESCE((SELECT SUM(amount) FROM captured), 0) AS actual"
    ),
}

MART_METRIC_QUERIES: dict[str, str] = {
    "net_revenue_raw_vs_mart": (
        "WITH latest_orders AS (SELECT DISTINCT ON (order_id) order_id, ordered_at "
        "FROM raw.orders ORDER BY order_id, logical_date DESC), "
        "latest_payments AS (SELECT DISTINCT ON (payment_id) payment_id, order_id, amount, status "
        "FROM raw.payments ORDER BY payment_id, logical_date DESC), "
        "raw_net AS (SELECT COALESCE(SUM(CASE WHEN p.status = 'captured' THEN p.amount "
        "WHEN p.status = 'refunded' THEN -p.amount ELSE 0 END), 0) AS value "
        "FROM latest_payments p JOIN latest_orders o ON o.order_id = p.order_id "
        "WHERE (o.ordered_at AT TIME ZONE 'UTC')::date = %(d)s) "
        "SELECT (SELECT value FROM raw_net) AS expected, "
        "COALESCE((SELECT net_revenue FROM marts.fct_daily_revenue WHERE order_date = %(d)s), 0) "
        "AS actual"
    ),
    "mart_daily_row_present": (
        "SELECT COUNT(*) AS value FROM marts.fct_daily_revenue WHERE order_date = %(d)s"
    ),
}


def _scalar(conn: Any, query: str, logical_date: date) -> Any:
    row = conn.execute(query, {"d": logical_date}).fetchone()
    return row[0] if row else None


def run_raw_checks(
    conn: Any, logical_date: date, thresholds: QualityThresholds
) -> list[CheckResult]:
    q = RAW_METRIC_QUERIES
    results: list[CheckResult] = []

    batches = conn.execute(q["batches"], {"d": logical_date}).fetchall()
    reasons: dict[str, int] = {}
    loaded = set()
    for dataset, read, rejected, reject_reasons in batches:
        loaded.add(dataset)
        results.append(check_reject_rate(dataset, int(read), int(rejected), thresholds))
        for reason, count in (reject_reasons or {}).items():
            reasons[f"{dataset}.{reason}"] = reasons.get(f"{dataset}.{reason}", 0) + int(count)
    missing = sorted({"customers", "products", "orders", "payments"} - loaded)
    results.append(
        check_zero(
            "all_datasets_loaded",
            len(missing),
            Severity.BLOCKING,
            f"dataset(s) not loaded: {', '.join(missing) or 'none'}",
        )
    )
    results.append(check_rejections_by_reason(reasons))

    results.append(
        check_zero(
            "duplicate_business_keys",
            int(_scalar(conn, q["duplicate_keys_in_partition"], logical_date)),
            Severity.BLOCKING,
            "business keys stored more than once for this date",
        )
    )
    results.append(
        check_zero(
            "orders_already_loaded_on_earlier_date",
            int(_scalar(conn, q["orders_seen_on_earlier_dates"], logical_date)),
            Severity.WARNING,
            "order_id values that also appear on an earlier date (latest version wins)",
        )
    )

    orders = int(_scalar(conn, q["orders_total"], logical_date))
    payments = int(_scalar(conn, q["payments_total"], logical_date))
    results.append(
        check_orphans(
            "orders_customer_fk",
            int(_scalar(conn, q["orders_orphan_customer"], logical_date)),
            orders,
            "orders reference an unknown customer",
            thresholds,
        )
    )
    results.append(
        check_orphans(
            "orders_product_fk",
            int(_scalar(conn, q["orders_orphan_product"], logical_date)),
            orders,
            "orders reference an unknown product",
            thresholds,
        )
    )
    results.append(
        check_orphans(
            "payments_order_fk",
            int(_scalar(conn, q["payments_orphan_order"], logical_date)),
            payments,
            "payments reference an unknown order",
            thresholds,
        )
    )

    history = [
        int(row[1])
        for row in conn.execute(q["order_count_history"], {"d": logical_date}).fetchall()
    ]
    results.append(check_row_count_change(orders, history, thresholds))

    expected, actual = conn.execute(q["order_value_vs_payments"], {"d": logical_date}).fetchone()
    results.append(
        check_revenue_reconciliation(
            "order_value_vs_captured_payments",
            Decimal(str(expected)),
            Decimal(str(actual)),
            thresholds,
            "non-cancelled order value vs captured payments for the same orders",
        )
    )
    return results


def run_mart_checks(
    conn: Any, logical_date: date, thresholds: QualityThresholds
) -> list[CheckResult]:
    q = MART_METRIC_QUERIES
    present = int(_scalar(conn, q["mart_daily_row_present"], logical_date))
    results = [
        CheckResult(
            "mart_daily_revenue_row_present",
            Severity.BLOCKING,
            Outcome.PASS if present == 1 else Outcome.FAIL,
            _dec(present),
            _dec(1),
            f"{present} row(s) in marts.fct_daily_revenue for {logical_date}",
        )
    ]
    expected, actual = conn.execute(q["net_revenue_raw_vs_mart"], {"d": logical_date}).fetchone()
    expected_dec, actual_dec = Decimal(str(expected)), Decimal(str(actual))
    exact = expected_dec == actual_dec
    results.append(
        CheckResult(
            "net_revenue_raw_vs_mart",
            Severity.BLOCKING,
            Outcome.PASS if exact else Outcome.FAIL,
            _dec(abs(expected_dec - actual_dec)),
            _dec(0),
            f"net revenue from raw payments {expected_dec}, from mart {actual_dec}",
        )
    )
    return results


def blocking_failures(results: Sequence[CheckResult]) -> list[CheckResult]:
    return [item for item in results if item.is_blocking_failure]
