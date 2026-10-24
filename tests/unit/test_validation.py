from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from dataops_sentinel.errors import SourceFileError
from dataops_sentinel.ingestion.reader import read_source_file
from dataops_sentinel.ingestion.schemas import CUSTOMERS, ORDERS, PAYMENTS, PRODUCTS
from dataops_sentinel.validation.records import RejectReason, validate_file, validate_row
from tests.conftest import FIXTURE_DATE


def reasons_by_key(result):
    return {record.business_key: record.reasons for record in result.rejected}


def test_orders_fixture_accepts_valid_rows_and_rejects_each_problem(landing_dir) -> None:
    result = validate_file(read_source_file(landing_dir, FIXTURE_DATE, ORDERS))
    assert result.records_read == 10
    assert [row["order_id"] for row in result.accepted] == ["O00000001", "O00000009"]
    reasons = reasons_by_key(result)
    assert reasons["O00000002"] == (RejectReason.INVALID_STATUS,)
    assert reasons["O00000003"] == (RejectReason.MISSING_REQUIRED_FIELD,)
    assert reasons["O00000004"] == (RejectReason.NEGATIVE_AMOUNT,)
    assert reasons["O00000005"] == (RejectReason.INVALID_TIMESTAMP,)
    assert reasons["O00000006"] == (RejectReason.TIMESTAMP_OUT_OF_RANGE,)
    assert reasons["O00000007"] == (RejectReason.INCONSISTENT_TOTAL,)
    assert reasons["O00000008"] == (RejectReason.INVALID_TIMESTAMP,)
    assert len(result.rejected) == 8
    assert result.reject_reasons["duplicate_key"] == 1


def test_duplicate_keeps_first_occurrence_and_rejects_later_ones(landing_dir) -> None:
    result = validate_file(read_source_file(landing_dir, FIXTURE_DATE, ORDERS))
    duplicates = [r for r in result.rejected if RejectReason.DUPLICATE_KEY in r.reasons]
    assert [(d.business_key, d.row_number) for d in duplicates] == [("O00000001", 9)]


def test_accepted_rows_are_typed(landing_dir) -> None:
    row = validate_file(read_source_file(landing_dir, FIXTURE_DATE, ORDERS)).accepted[0]
    assert row["quantity"] == 2
    assert row["order_total"] == Decimal("39.98")
    assert row["ordered_at"] == datetime(2026, 1, 2, 10, 15, tzinfo=UTC)


def test_customers_missing_email_invalid_country_and_duplicate(landing_dir) -> None:
    result = validate_file(read_source_file(landing_dir, FIXTURE_DATE, CUSTOMERS))
    assert [row["customer_id"] for row in result.accepted] == ["C00001"]
    assert sorted(result.reject_reasons.items()) == [
        ("duplicate_key", 1),
        ("invalid_format", 1),
        ("missing_required_field", 1),
    ]


def test_products_non_numeric_price_is_invalid_type(landing_dir) -> None:
    result = validate_file(read_source_file(landing_dir, FIXTURE_DATE, PRODUCTS))
    assert reasons_by_key(result)["P0002"] == (RejectReason.INVALID_TYPE,)


def test_payment_window_allows_next_day_and_rejects_negative_amount(landing_dir) -> None:
    result = validate_file(read_source_file(landing_dir, FIXTURE_DATE, PAYMENTS))
    assert [row["payment_id"] for row in result.accepted] == ["PAY00000001", "PAY00000002"]
    assert reasons_by_key(result)["PAY00000003"] == (RejectReason.NEGATIVE_AMOUNT,)


def test_missing_required_column_is_a_file_level_error(landing_dir) -> None:
    with pytest.raises(SourceFileError, match=r"missing required columns \['status'\]"):
        read_source_file(landing_dir, date(2026, 1, 3), ORDERS)


def test_missing_file_is_a_file_level_error(landing_dir) -> None:
    with pytest.raises(SourceFileError, match="not found"):
        read_source_file(landing_dir, date(2026, 1, 3), PAYMENTS)


def test_multiple_problems_on_one_row_are_all_reported() -> None:
    typed, problems = validate_row(
        ORDERS,
        FIXTURE_DATE,
        {
            "order_id": "O00000001",
            "customer_id": "",
            "product_id": "P1",
            "quantity": "x",
            "unit_price": "1.00",
            "order_total": "1.00",
            "status": "unknown",
            "ordered_at": "2026-01-02T10:00:00Z",
        },
    )
    assert typed is None
    assert {reason for reason, _ in problems} == {
        RejectReason.MISSING_REQUIRED_FIELD,
        RejectReason.INVALID_FORMAT,
        RejectReason.INVALID_TYPE,
        RejectReason.INVALID_STATUS,
    }


@pytest.mark.parametrize("amount", ["1.234", "NaN", "Infinity"])
def test_decimal_precision_and_finiteness(amount: str) -> None:
    typed, problems = validate_row(
        PAYMENTS,
        FIXTURE_DATE,
        {
            "payment_id": "PAY00000001",
            "order_id": "O00000001",
            "amount": amount,
            "method": "card",
            "status": "captured",
            "paid_at": "2026-01-02T10:00:00Z",
        },
    )
    assert typed is None
    assert problems[0][0] is RejectReason.INVALID_TYPE


def test_extra_values_on_a_row_are_malformed(tmp_path) -> None:
    folder = tmp_path / FIXTURE_DATE.isoformat()
    folder.mkdir()
    (folder / "products.csv").write_text(
        "product_id,product_name,category,unit_price,updated_at\n"
        "P0001,Product,home,1.00,2026-01-02T00:00:00Z,surplus\n"
    )
    result = validate_file(read_source_file(tmp_path, FIXTURE_DATE, PRODUCTS))
    assert result.rejected[0].reasons == (RejectReason.MALFORMED_ROW,)


def test_unexpected_columns_are_reported_not_rejected(tmp_path) -> None:
    folder = tmp_path / FIXTURE_DATE.isoformat()
    folder.mkdir()
    (folder / "products.csv").write_text(
        "product_id,product_name,category,unit_price,updated_at,color\n"
        "P0001,Product,home,1.00,2026-01-02T00:00:00Z,red\n"
    )
    source = read_source_file(tmp_path, FIXTURE_DATE, PRODUCTS)
    assert source.unexpected_columns == ("color",)
    assert len(validate_file(source).accepted) == 1


def test_reject_rate() -> None:
    from dataops_sentinel.validation.records import ValidationResult

    assert ValidationResult("orders", FIXTURE_DATE).reject_rate == 0.0
