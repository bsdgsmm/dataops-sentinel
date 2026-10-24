"""Row-level validation of a source file.

Each row is either accepted (converted to typed values) or rejected with one
or more reason codes. Rejected rows are kept, never silently dropped.
"""

from __future__ import annotations

import json
import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Any

from dataops_sentinel.ingestion.reader import SourceFile
from dataops_sentinel.ingestion.schemas import ORDERS, Column, ColumnType, DatasetSpec

ORDER_TOTAL_TOLERANCE = Decimal("0.01")
CENT = Decimal("0.01")


class RejectReason(StrEnum):
    MISSING_REQUIRED_FIELD = "missing_required_field"
    INVALID_TYPE = "invalid_type"
    INVALID_FORMAT = "invalid_format"
    INVALID_TIMESTAMP = "invalid_timestamp"
    TIMESTAMP_OUT_OF_RANGE = "timestamp_out_of_range"
    INVALID_STATUS = "invalid_status"
    NEGATIVE_AMOUNT = "negative_amount"
    INCONSISTENT_TOTAL = "inconsistent_total"
    DUPLICATE_KEY = "duplicate_key"
    MALFORMED_ROW = "malformed_row"


@dataclass(frozen=True)
class RejectedRecord:
    dataset: str
    logical_date: date
    row_number: int
    business_key: str | None
    reasons: tuple[RejectReason, ...]
    messages: tuple[str, ...]
    raw: dict[str, str]

    def raw_json(self) -> str:
        return json.dumps(self.raw, sort_keys=True)


@dataclass
class ValidationResult:
    dataset: str
    logical_date: date
    records_read: int = 0
    accepted: list[dict[str, Any]] = field(default_factory=list)
    rejected: list[RejectedRecord] = field(default_factory=list)

    @property
    def reject_reasons(self) -> Counter[str]:
        return Counter(reason.value for record in self.rejected for reason in record.reasons)

    @property
    def reject_rate(self) -> float:
        return len(self.rejected) / self.records_read if self.records_read else 0.0


class _Invalid(Exception):
    def __init__(self, reason: RejectReason, message: str) -> None:
        super().__init__(message)
        self.reason = reason


def parse_timestamp(value: str) -> datetime:
    """Parse ISO-8601 with an explicit offset; naive timestamps are invalid."""
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise _Invalid(
            RejectReason.INVALID_TIMESTAMP, f"not an ISO-8601 timestamp: {value!r}"
        ) from exc
    if parsed.tzinfo is None:
        raise _Invalid(RejectReason.INVALID_TIMESTAMP, f"timestamp has no UTC offset: {value!r}")
    return parsed.astimezone(UTC)


def _convert(column: Column, value: str) -> Any:
    if column.type is ColumnType.TEXT:
        if column.allowed_values is not None and value not in column.allowed_values:
            reason = (
                RejectReason.INVALID_STATUS
                if column.name == "status"
                else RejectReason.INVALID_FORMAT
            )
            raise _Invalid(reason, f"{column.name}={value!r} is not an allowed value")
        if column.pattern is not None and not re.fullmatch(column.pattern, value):
            raise _Invalid(
                RejectReason.INVALID_FORMAT, f"{column.name}={value!r} has an invalid format"
            )
        return value
    if column.type is ColumnType.INTEGER:
        try:
            number = int(value)
        except ValueError as exc:
            raise _Invalid(
                RejectReason.INVALID_TYPE, f"{column.name}={value!r} is not an integer"
            ) from exc
        if column.non_negative and number < 0:
            raise _Invalid(RejectReason.NEGATIVE_AMOUNT, f"{column.name}={number} is negative")
        return number
    if column.type is ColumnType.DECIMAL:
        try:
            number = Decimal(value)
        except InvalidOperation as exc:
            raise _Invalid(
                RejectReason.INVALID_TYPE, f"{column.name}={value!r} is not a decimal"
            ) from exc
        if not number.is_finite():
            raise _Invalid(RejectReason.INVALID_TYPE, f"{column.name}={value!r} is not finite")
        if number.as_tuple().exponent < -2:
            raise _Invalid(
                RejectReason.INVALID_TYPE, f"{column.name}={value!r} has more than 2 decimals"
            )
        if column.non_negative and number < 0:
            raise _Invalid(RejectReason.NEGATIVE_AMOUNT, f"{column.name}={number} is negative")
        return number.quantize(CENT)
    return parse_timestamp(value)


def _day_start(logical_date: date) -> datetime:
    return datetime.combine(logical_date, time.min, tzinfo=UTC)


def validate_row(
    spec: DatasetSpec, logical_date: date, raw: dict[str, str]
) -> tuple[dict[str, Any] | None, list[tuple[RejectReason, str]]]:
    problems: list[tuple[RejectReason, str]] = []
    if raw.get("__extra_values__"):
        problems.append((RejectReason.MALFORMED_ROW, "row has more values than the header"))
    typed: dict[str, Any] = {}
    for column in spec.columns:
        value = raw.get(column.name, "")
        if value == "":
            if column.required:
                problems.append((RejectReason.MISSING_REQUIRED_FIELD, f"{column.name} is required"))
            typed[column.name] = None
            continue
        try:
            typed[column.name] = _convert(column, value)
        except _Invalid as invalid:
            problems.append((invalid.reason, str(invalid)))

    window = spec.timestamp_window
    stamp = typed.get(window.column)
    if isinstance(stamp, datetime):
        start = _day_start(logical_date) - window.starts_before
        end = _day_start(logical_date) + window.ends_after
        if not start <= stamp < end:
            problems.append(
                (
                    RejectReason.TIMESTAMP_OUT_OF_RANGE,
                    f"{window.column}={stamp.isoformat()} outside [{start.isoformat()}, "
                    f"{end.isoformat()})",
                )
            )

    if spec is ORDERS and not problems:
        expected = (typed["unit_price"] * typed["quantity"]).quantize(CENT)
        if abs(expected - typed["order_total"]) > ORDER_TOTAL_TOLERANCE:
            problems.append(
                (
                    RejectReason.INCONSISTENT_TOTAL,
                    f"order_total={typed['order_total']} but quantity*unit_price={expected}",
                )
            )
    return (None if problems else typed), problems


def validate_file(source: SourceFile) -> ValidationResult:
    spec = source.spec
    result = ValidationResult(spec.name, source.logical_date, records_read=len(source.rows))
    seen_keys: set[str] = set()
    for row_number, raw in source.rows:
        typed, problems = validate_row(spec, source.logical_date, raw)
        key = raw.get(spec.business_key) or None
        if typed is not None and key in seen_keys:
            typed = None
            problems = [(RejectReason.DUPLICATE_KEY, f"{spec.business_key}={key} already in file")]
        if typed is None:
            result.rejected.append(
                RejectedRecord(
                    dataset=spec.name,
                    logical_date=source.logical_date,
                    row_number=row_number,
                    business_key=key,
                    reasons=tuple(dict.fromkeys(reason for reason, _ in problems)),
                    messages=tuple(message for _, message in problems),
                    raw={k: v for k, v in raw.items() if not k.startswith("__")},
                )
            )
            continue
        seen_keys.add(key)
        result.accepted.append(typed)
    return result
