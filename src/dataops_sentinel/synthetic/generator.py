"""Deterministic synthetic marketplace data, one landing folder per logical date.

All values are invented. The same (seed, logical_date, scenario) always
produces byte-identical files, and the state for date D is derived by
replaying every date from ``START_DATE`` up to D, so any historical date can
be regenerated independently.

Scenarios:

* ``clean``          - no injected faults;
* ``default``        - a small share of invalid rows (missing fields, bad
                       statuses, negative amounts, bad timestamps, duplicates,
                       unknown foreign keys) that stays below the blocking
                       reject-rate threshold;
* ``dirty``          - many invalid order rows, so the reject-rate gate fails;
* ``missing_column`` - orders.csv without the ``status`` column, a contract
                       violation that fails ingestion without retries.
"""

from __future__ import annotations

import argparse
import csv
import logging
import random
import sys
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path

from dataops_sentinel.ingestion.schemas import CUSTOMERS, ORDERS, PAYMENTS, PRODUCTS, DatasetSpec

logger = logging.getLogger(__name__)

START_DATE = date(2026, 1, 1)
SCENARIOS = ("clean", "default", "dirty", "missing_column")
CATEGORIES = ("electronics", "home", "books", "sports", "beauty", "toys")
COUNTRY_WEIGHTS = {"DE": 30, "FR": 20, "NL": 10, "ES": 12, "IT": 12, "TR": 8, "PL": 5, "SE": 3}
FIRST_NAMES = ("Alex", "Sam", "Robin", "Kim", "Jules", "Noa", "Mika", "Eli", "Ari", "Lou")
LAST_NAMES = ("Berg", "Novak", "Silva", "Kaya", "Moreau", "Rossi", "Jansen", "Lind", "Duran")
CENT = Decimal("0.01")


@dataclass
class MarketState:
    customer_count: int = 0
    product_prices: dict[str, Decimal] = field(default_factory=dict)
    order_count: int = 0
    payment_count: int = 0


@dataclass
class DayFiles:
    logical_date: date
    customers: list[dict[str, str]]
    products: list[dict[str, str]]
    orders: list[dict[str, str]]
    payments: list[dict[str, str]]


def _iso(moment: datetime) -> str:
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _day_rng(seed: int, logical_date: date) -> random.Random:
    return random.Random(f"{seed}:{logical_date.isoformat()}")


def _moment(rng: random.Random, logical_date: date) -> datetime:
    return datetime.combine(logical_date, time.min, tzinfo=UTC) + timedelta(
        seconds=rng.randrange(0, 86_400)
    )


def _build_day(state: MarketState, seed: int, logical_date: date) -> DayFiles:
    rng = _day_rng(seed, logical_date)
    first_day = logical_date == START_DATE

    customers = []
    new_customers = 150 if first_day else rng.randint(5, 15)
    for _ in range(new_customers):
        state.customer_count += 1
        number = state.customer_count
        first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
        customers.append(
            {
                "customer_id": f"C{number:05d}",
                "email": f"{first.lower()}.{last.lower()}{number}@example.com",
                "full_name": f"{first} {last}",
                "country": rng.choices(list(COUNTRY_WEIGHTS), list(COUNTRY_WEIGHTS.values()))[0],
                "created_at": _iso(_moment(rng, logical_date)),
            }
        )

    products = []
    if first_day:
        for number in range(1, 61):
            price = (Decimal(rng.randint(500, 25_000)) / 100).quantize(CENT)
            state.product_prices[f"P{number:04d}"] = price
            products.append((f"P{number:04d}", price))
    else:
        for product_id in rng.sample(sorted(state.product_prices), k=rng.randint(0, 3)):
            factor = Decimal(str(rng.choice([0.9, 0.95, 1.05, 1.1])))
            state.product_prices[product_id] = (state.product_prices[product_id] * factor).quantize(
                CENT
            )
            products.append((product_id, state.product_prices[product_id]))
    product_rows = [
        {
            "product_id": product_id,
            "product_name": f"Product {product_id[1:]}",
            "category": CATEGORIES[int(product_id[1:]) % len(CATEGORIES)],
            "unit_price": str(price),
            "updated_at": _iso(_moment(rng, logical_date)),
        }
        for product_id, price in products
    ]

    weekday_factor = (1.0, 0.95, 0.95, 1.0, 1.15, 1.35, 1.25)[logical_date.weekday()]
    order_count = max(1, int(rng.gauss(120, 12) * weekday_factor))
    product_ids = sorted(state.product_prices)
    orders, payments = [], []
    for _ in range(order_count):
        state.order_count += 1
        order_id = f"O{state.order_count:08d}"
        product_id = rng.choice(product_ids)
        quantity = rng.choices((1, 2, 3, 4), (60, 25, 10, 5))[0]
        unit_price = state.product_prices[product_id]
        total = (unit_price * quantity).quantize(CENT)
        status = rng.choices(("placed", "shipped", "delivered", "cancelled"), (20, 30, 42, 8))[0]
        ordered_at = _moment(rng, logical_date)
        orders.append(
            {
                "order_id": order_id,
                "customer_id": f"C{rng.randint(1, state.customer_count):05d}",
                "product_id": product_id,
                "quantity": str(quantity),
                "unit_price": str(unit_price),
                "order_total": str(total),
                "status": status,
                "ordered_at": _iso(ordered_at),
            }
        )
        if status == "cancelled":
            continue
        method = rng.choice(("card", "card", "bank_transfer", "wallet"))
        paid_at = ordered_at + timedelta(minutes=rng.randint(1, 240))
        if rng.random() < 0.05:
            state.payment_count += 1
            payments.append(
                {
                    "payment_id": f"PAY{state.payment_count:08d}",
                    "order_id": order_id,
                    "amount": str(total),
                    "method": method,
                    "status": "failed",
                    "paid_at": _iso(paid_at - timedelta(minutes=1)),
                }
            )
        state.payment_count += 1
        payments.append(
            {
                "payment_id": f"PAY{state.payment_count:08d}",
                "order_id": order_id,
                "amount": str(total),
                "method": method,
                "status": "captured",
                "paid_at": _iso(paid_at),
            }
        )
        if status == "delivered" and rng.random() < 0.04:
            state.payment_count += 1
            payments.append(
                {
                    "payment_id": f"PAY{state.payment_count:08d}",
                    "order_id": order_id,
                    "amount": str(total),
                    "method": method,
                    "status": "refunded",
                    "paid_at": _iso(paid_at + timedelta(hours=rng.randint(1, 20))),
                }
            )
    return DayFiles(logical_date, customers, product_rows, orders, payments)


def _inject_faults(day: DayFiles, seed: int, scenario: str) -> None:
    if scenario == "clean":
        return
    rng = random.Random(f"{seed}:{day.logical_date.isoformat()}:faults")
    orders = day.orders
    faults = 3 if scenario == "default" else max(12, len(orders) // 5)
    for _ in range(faults):
        index = rng.randrange(len(orders))
        row = dict(orders[index])
        kind = rng.choice(
            ("missing", "status", "negative", "timestamp", "duplicate", "orphan", "total")
        )
        if kind == "missing":
            row["customer_id"] = ""
        elif kind == "status":
            row["status"] = "lost_in_transit"
        elif kind == "negative":
            row["quantity"] = "-1"
        elif kind == "timestamp":
            row["ordered_at"] = "2026-13-45T25:61:00Z"
        elif kind == "orphan":
            row["customer_id"] = "C99999"
        elif kind == "total":
            row["order_total"] = str(Decimal(row["order_total"]) + 10)
        if kind == "duplicate":
            orders.append(row)
        else:
            orders[index] = row
    if day.payments and scenario in {"default", "dirty"}:
        index = rng.randrange(len(day.payments))
        bad = dict(day.payments[index])
        bad["amount"] = "-" + bad["amount"]
        day.payments[index] = bad
    if len(day.customers) >= 100 and scenario in {"default", "dirty"}:
        index = rng.randrange(len(day.customers))
        day.customers[index] = {**day.customers[index], "email": ""}


def generate_day(seed: int, logical_date: date, scenario: str = "default") -> DayFiles:
    if scenario not in SCENARIOS:
        raise ValueError(f"unknown scenario {scenario!r}; choose one of {SCENARIOS}")
    if logical_date < START_DATE:
        raise ValueError(f"logical_date must be on or after {START_DATE}")
    state = MarketState()
    current = START_DATE
    while True:
        day = _build_day(state, seed, current)
        if current == logical_date:
            break
        current += timedelta(days=1)
    _inject_faults(day, seed, scenario)
    return day


def _write_csv(path: Path, spec: DatasetSpec, rows: list[dict[str, str]], drop: str | None) -> None:
    columns = [name for name in spec.column_names if name != drop]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=columns, extrasaction="ignore", lineterminator="\n"
        )
        writer.writeheader()
        writer.writerows(rows)


def write_day(day: DayFiles, landing_dir: Path, seed: int, scenario: str = "default") -> Path:
    folder = landing_dir / day.logical_date.isoformat()
    folder.mkdir(parents=True, exist_ok=True)
    _write_csv(folder / CUSTOMERS.file_name, CUSTOMERS, day.customers, None)
    _write_csv(folder / PRODUCTS.file_name, PRODUCTS, day.products, None)
    _write_csv(
        folder / ORDERS.file_name,
        ORDERS,
        day.orders,
        "status" if scenario == "missing_column" else None,
    )
    _write_csv(folder / PAYMENTS.file_name, PAYMENTS, day.payments, None)
    (folder / "_SYNTHETIC_DATA.txt").write_text(
        "All files in this folder are synthetic data generated by dataops-sentinel.\n"
        f"logical_date={day.logical_date.isoformat()} seed={seed} scenario={scenario}\n",
        encoding="utf-8",
    )
    return folder


def date_range(start: date, end: date) -> list[date]:
    if end < start:
        raise ValueError("end date must not be before start date")
    return [start + timedelta(days=offset) for offset in range((end - start).days + 1)]


def main(argv: list[str] | None = None) -> int:
    from dataops_sentinel.config import Settings
    from dataops_sentinel.logging_config import configure_logging

    settings = Settings.from_env()
    configure_logging(settings.log_level)
    parser = argparse.ArgumentParser(description="Generate synthetic landing files.")
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--scenario", choices=SCENARIOS, default="default")
    parser.add_argument("--landing-dir", type=Path, default=settings.landing_dir)
    args = parser.parse_args(argv)
    for logical_date in date_range(args.start, args.end or args.start):
        day = generate_day(args.seed, logical_date, args.scenario)
        folder = write_day(day, args.landing_dir, args.seed, args.scenario)
        logger.info(
            "Synthetic landing files written",
            extra={
                "folder": str(folder),
                "customers": len(day.customers),
                "products": len(day.products),
                "orders": len(day.orders),
                "payments": len(day.payments),
                "scenario": args.scenario,
            },
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
