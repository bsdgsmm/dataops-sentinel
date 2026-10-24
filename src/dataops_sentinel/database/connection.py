"""Connection management.

Connections are opened in autocommit mode; every write that must be atomic
runs inside an explicit ``conn.transaction()`` block, which commits on success
and rolls back on any exception.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Protocol

from dataops_sentinel.config import DatabaseSettings
from dataops_sentinel.errors import PermanentPipelineError, TransientPipelineError

logger = logging.getLogger(__name__)


class Cursor(Protocol):
    def execute(self, query: str, params: Any = ...) -> Any: ...

    def executemany(self, query: str, params_seq: Any) -> Any: ...

    def fetchone(self) -> Any: ...

    def fetchall(self) -> list[Any]: ...


class Connection(Protocol):
    def execute(self, query: str, params: Any = ...) -> Cursor: ...

    def cursor(self) -> Any: ...

    def transaction(self) -> Any: ...

    def close(self) -> None: ...


def classify_database_error(exc: BaseException) -> Exception:
    """Map driver errors onto transient (retry) or permanent (do not retry)."""
    try:
        import psycopg
    except ImportError:
        return PermanentPipelineError(str(exc))
    transient = (
        psycopg.OperationalError,
        psycopg.errors.SerializationFailure,
        psycopg.errors.DeadlockDetected,
        psycopg.errors.LockNotAvailable,
        psycopg.errors.QueryCanceled,
    )
    if isinstance(exc, transient):
        return TransientPipelineError(f"database temporarily unavailable: {exc}")
    return PermanentPipelineError(f"database error: {exc}")


def connect(settings: DatabaseSettings) -> Connection:
    import psycopg

    try:
        return psycopg.connect(**settings.conninfo(), autocommit=True)
    except psycopg.Error as exc:
        raise classify_database_error(exc) from exc


@contextmanager
def open_connection(settings: DatabaseSettings) -> Iterator[Connection]:
    conn = connect(settings)
    try:
        yield conn
    finally:
        conn.close()


def apply_schema(conn: Connection, schema_path: Path) -> None:
    with conn.transaction():
        conn.execute(schema_path.read_text())
    logger.info("Schema applied", extra={"path": str(schema_path)})
