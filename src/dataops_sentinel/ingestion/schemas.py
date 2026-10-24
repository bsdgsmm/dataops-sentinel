"""Source dataset contracts: columns, types, business keys and allowed values.

Every daily landing folder contains one CSV per dataset. Timestamps must carry
a UTC offset (``Z`` or ``+00:00``). Amounts are decimals with two places.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum


class ColumnType(StrEnum):
    TEXT = "text"
    INTEGER = "integer"
    DECIMAL = "decimal"
    TIMESTAMP = "timestamp"


@dataclass(frozen=True)
class Column:
    name: str
    type: ColumnType
    required: bool = True
    allowed_values: frozenset[str] | None = None
    non_negative: bool = False
    pattern: str | None = None


@dataclass(frozen=True)
class TimestampWindow:
    """Accepted range for a timestamp relative to the logical date (UTC midnight)."""

    column: str
    starts_before: timedelta
    ends_after: timedelta


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    file_name: str
    table: str
    columns: tuple[Column, ...]
    business_key: str
    timestamp_window: TimestampWindow

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.columns)

    @property
    def required_columns(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.columns if column.required)


ORDER_STATUSES = frozenset({"placed", "shipped", "delivered", "cancelled"})
PAYMENT_STATUSES = frozenset({"captured", "refunded", "failed"})
PAYMENT_METHODS = frozenset({"card", "bank_transfer", "wallet"})
COUNTRIES = frozenset({"DE", "FR", "NL", "ES", "IT", "TR", "PL", "SE"})

CUSTOMERS = DatasetSpec(
    name="customers",
    file_name="customers.csv",
    table="raw.customers",
    business_key="customer_id",
    columns=(
        Column("customer_id", ColumnType.TEXT, pattern=r"^C\d{5}$"),
        Column("email", ColumnType.TEXT, pattern=r"^[^@\s]+@[^@\s]+\.[a-z]{2,}$"),
        Column("full_name", ColumnType.TEXT),
        Column("country", ColumnType.TEXT, allowed_values=COUNTRIES),
        Column("created_at", ColumnType.TIMESTAMP),
    ),
    timestamp_window=TimestampWindow("created_at", timedelta(days=3650), timedelta(days=1)),
)

PRODUCTS = DatasetSpec(
    name="products",
    file_name="products.csv",
    table="raw.products",
    business_key="product_id",
    columns=(
        Column("product_id", ColumnType.TEXT, pattern=r"^P\d{4}$"),
        Column("product_name", ColumnType.TEXT),
        Column("category", ColumnType.TEXT),
        Column("unit_price", ColumnType.DECIMAL, non_negative=True),
        Column("updated_at", ColumnType.TIMESTAMP),
    ),
    timestamp_window=TimestampWindow("updated_at", timedelta(days=3650), timedelta(days=1)),
)

ORDERS = DatasetSpec(
    name="orders",
    file_name="orders.csv",
    table="raw.orders",
    business_key="order_id",
    columns=(
        Column("order_id", ColumnType.TEXT, pattern=r"^O\d{8}$"),
        Column("customer_id", ColumnType.TEXT, pattern=r"^C\d{5}$"),
        Column("product_id", ColumnType.TEXT, pattern=r"^P\d{4}$"),
        Column("quantity", ColumnType.INTEGER, non_negative=True),
        Column("unit_price", ColumnType.DECIMAL, non_negative=True),
        Column("order_total", ColumnType.DECIMAL, non_negative=True),
        Column("status", ColumnType.TEXT, allowed_values=ORDER_STATUSES),
        Column("ordered_at", ColumnType.TIMESTAMP),
    ),
    timestamp_window=TimestampWindow("ordered_at", timedelta(0), timedelta(days=1)),
)

PAYMENTS = DatasetSpec(
    name="payments",
    file_name="payments.csv",
    table="raw.payments",
    business_key="payment_id",
    columns=(
        Column("payment_id", ColumnType.TEXT, pattern=r"^PAY\d{8}$"),
        Column("order_id", ColumnType.TEXT, pattern=r"^O\d{8}$"),
        Column("amount", ColumnType.DECIMAL, non_negative=True),
        Column("method", ColumnType.TEXT, allowed_values=PAYMENT_METHODS),
        Column("status", ColumnType.TEXT, allowed_values=PAYMENT_STATUSES),
        Column("paid_at", ColumnType.TIMESTAMP),
    ),
    timestamp_window=TimestampWindow("paid_at", timedelta(0), timedelta(days=2)),
)

DATASETS: dict[str, DatasetSpec] = {
    spec.name: spec for spec in (CUSTOMERS, PRODUCTS, ORDERS, PAYMENTS)
}
LOAD_ORDER: tuple[str, ...] = ("customers", "products", "orders", "payments")
