from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pytest

from dataops_sentinel.ingestion.reader import read_source_file
from dataops_sentinel.ingestion.schemas import DATASETS, ORDERS
from dataops_sentinel.synthetic.generator import (
    START_DATE,
    date_range,
    generate_day,
    write_day,
)
from dataops_sentinel.validation.records import validate_file


def test_same_inputs_produce_identical_files(tmp_path) -> None:
    for folder in ("a", "b"):
        write_day(generate_day(7, date(2026, 1, 4)), tmp_path / folder, 7)
    for spec in DATASETS.values():
        first = (tmp_path / "a" / "2026-01-04" / spec.file_name).read_bytes()
        second = (tmp_path / "b" / "2026-01-04" / spec.file_name).read_bytes()
        assert first == second


def test_historical_date_is_independent_of_generation_order() -> None:
    late_first = generate_day(7, date(2026, 1, 5))
    generate_day(7, date(2026, 1, 9))
    again = generate_day(7, date(2026, 1, 5))
    assert late_first.orders == again.orders


def test_identifiers_continue_across_days() -> None:
    day1 = generate_day(7, START_DATE, "clean")
    day2 = generate_day(7, START_DATE + timedelta(days=1), "clean")
    last_day1 = max(int(row["order_id"][1:]) for row in day1.orders)
    first_day2 = min(int(row["order_id"][1:]) for row in day2.orders)
    assert first_day2 == last_day1 + 1


def test_clean_scenario_validates_without_rejections(tmp_path) -> None:
    write_day(generate_day(7, date(2026, 1, 3), "clean"), tmp_path, 7, "clean")
    for spec in DATASETS.values():
        result = validate_file(read_source_file(tmp_path, date(2026, 1, 3), spec))
        assert result.rejected == [], spec.name


def test_order_totals_and_captured_payments_reconcile_in_clean_data() -> None:
    day = generate_day(7, date(2026, 1, 2), "clean")
    totals = {row["order_id"]: Decimal(row["order_total"]) for row in day.orders}
    for row in day.orders:
        assert Decimal(row["unit_price"]) * int(row["quantity"]) == Decimal(row["order_total"])
    captured = {}
    for payment in day.payments:
        if payment["status"] == "captured":
            captured[payment["order_id"]] = captured.get(payment["order_id"], 0) + Decimal(
                payment["amount"]
            )
    expected = {
        k: v
        for k, v in totals.items()
        if next(o["status"] for o in day.orders if o["order_id"] == k) != "cancelled"
    }
    assert captured == expected


def test_default_scenario_stays_below_blocking_reject_rate(tmp_path) -> None:
    for day in date_range(date(2026, 1, 1), date(2026, 1, 7)):
        write_day(generate_day(7, day), tmp_path, 7)
        result = validate_file(read_source_file(tmp_path, day, ORDERS))
        assert 0 < result.reject_rate <= 0.05


def test_dirty_scenario_exceeds_blocking_reject_rate(tmp_path) -> None:
    write_day(generate_day(7, date(2026, 1, 3), "dirty"), tmp_path, 7, "dirty")
    assert validate_file(read_source_file(tmp_path, date(2026, 1, 3), ORDERS)).reject_rate > 0.05


def test_missing_column_scenario_drops_status(tmp_path) -> None:
    write_day(generate_day(7, date(2026, 1, 3), "missing_column"), tmp_path, 7, "missing_column")
    header = (tmp_path / "2026-01-03" / "orders.csv").read_text().splitlines()[0]
    assert "status" not in header.split(",")


def test_files_are_marked_synthetic(tmp_path) -> None:
    folder = write_day(generate_day(7, START_DATE), tmp_path, 7)
    assert "synthetic" in (folder / "_SYNTHETIC_DATA.txt").read_text()


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"logical_date": date(2025, 12, 31)}, "on or after"),
        ({"scenario": "chaos"}, "unknown scenario"),
    ],
)
def test_invalid_arguments(kwargs, message) -> None:
    args = {"seed": 7, "logical_date": START_DATE, "scenario": "default"} | kwargs
    with pytest.raises(ValueError, match=message):
        generate_day(**args)


def test_date_range() -> None:
    assert date_range(date(2026, 1, 1), date(2026, 1, 3))[-1] == date(2026, 1, 3)
    with pytest.raises(ValueError):
        date_range(date(2026, 1, 3), date(2026, 1, 1))
