"""In-memory stand-in for a database connection used by unit tests.

It understands only the statement shapes the repository issues: deletes by
partition and inserts. Statements inside ``transaction()`` are applied on
successful exit and discarded on an exception, like a real transaction.
"""

from __future__ import annotations

import re
from collections import defaultdict
from contextlib import contextmanager
from typing import Any

_DELETE = re.compile(r"DELETE FROM ([\w.]+) WHERE (.+)")
_INSERT = re.compile(r"INSERT INTO ([\w.]+) \(([^)]*)\)")


class FakeCursor:
    def __init__(self, conn: FakeConnection, rows: list[tuple] | None = None) -> None:
        self.conn = conn
        self.rows = rows or []
        self.rowcount = 0
        self.description: list[tuple[str]] = []

    def execute(self, query: str, params: Any = None) -> FakeCursor:
        return self.conn.execute(query, params)

    def executemany(self, query: str, params_seq: Any) -> None:
        for params in params_seq:
            self.conn.execute(query, params)

    def fetchone(self) -> Any:
        return self.rows[0] if self.rows else None

    def fetchall(self) -> list[Any]:
        return list(self.rows)

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *exc: object) -> bool:
        return False


class FakeConnection:
    def __init__(self, fail_on: str | None = None) -> None:
        self.tables: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self.pending: list[tuple[str, Any]] | None = None
        self.statements: list[str] = []
        self.commits = 0
        self.rollbacks = 0
        self.fail_on = fail_on
        self.closed = False
        self.query_results: dict[str, list[tuple]] = {}

    def _apply(self, query: str, params: Any) -> int:
        if self.fail_on and self.fail_on in query:
            raise ConnectionError(f"simulated failure on: {self.fail_on}")
        match = _DELETE.match(query)
        if match:
            table, where = match.groups()
            conditions = [part.split("=")[0].strip() for part in where.split(" AND ")]
            keep, removed = [], 0
            for row in self.tables[table]:
                if all(
                    row.get(column) == value
                    for column, value in zip(conditions, params, strict=True)
                ):
                    removed += 1
                else:
                    keep.append(row)
            self.tables[table] = keep
            return removed
        match = _INSERT.match(query)
        if match:
            table, columns = match.groups()
            names = [name.strip() for name in columns.split(",")]
            row = dict(zip(names, params, strict=False))
            if "ON CONFLICT" in query:
                key = ("dataset", "logical_date")
                self.tables[table] = [
                    existing
                    for existing in self.tables[table]
                    if tuple(existing.get(k) for k in key) != tuple(row.get(k) for k in key)
                ]
            self.tables[table].append(row)
            return 1
        return 0

    def execute(self, query: str, params: Any = None) -> FakeCursor:
        self.statements.append(query)
        for marker, rows in self.query_results.items():
            if marker in query:
                return FakeCursor(self, rows)
        cursor = FakeCursor(self)
        if self.pending is not None:
            if self.fail_on and self.fail_on in query:
                raise ConnectionError(f"simulated failure on: {self.fail_on}")
            self.pending.append((query, params))
            if query.startswith("DELETE"):
                cursor.rowcount = self._count_matches(query, params)
        else:
            cursor.rowcount = self._apply(query, params)
        return cursor

    def _count_matches(self, query: str, params: Any) -> int:
        match = _DELETE.match(query)
        if not match:
            return 0
        table, where = match.groups()
        conditions = [part.split("=")[0].strip() for part in where.split(" AND ")]
        return sum(
            all(row.get(c) == v for c, v in zip(conditions, params, strict=True))
            for row in self.tables[table]
        )

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    @contextmanager
    def transaction(self):
        if self.pending is not None:
            yield
            return
        self.pending = []
        try:
            yield
        except BaseException:
            self.pending = None
            self.rollbacks += 1
            raise
        operations, self.pending = self.pending, None
        for query, params in operations:
            self._apply(query, params)
        self.commits += 1

    def close(self) -> None:
        self.closed = True
