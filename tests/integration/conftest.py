"""PostgreSQL-backed fixtures. Enabled with SENTINEL_RUN_INTEGRATION=1.

Each test session creates a throwaway database, applies sql/schema.sql and
drops the database afterwards. The configured user needs CREATEDB.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path

import pytest

from dataops_sentinel.config import PROJECT_ROOT, Settings

RUN = os.environ.get("SENTINEL_RUN_INTEGRATION") == "1"


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    skip = pytest.mark.skip(reason="set SENTINEL_RUN_INTEGRATION=1 with PostgreSQL running")
    for item in items:
        if "tests/integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)
            if not RUN:
                item.add_marker(skip)


@pytest.fixture(scope="session")
def database_settings() -> Iterator[Settings]:
    import psycopg

    base = Settings.from_env()
    name = f"sentinel_it_{uuid.uuid4().hex[:8]}"
    admin = replace(base.database, dbname=os.environ.get("SENTINEL_ADMIN_DB", "postgres"))
    with psycopg.connect(**admin.conninfo(), autocommit=True) as conn:
        conn.execute(f"CREATE DATABASE {name}")
    settings = replace(base, database=replace(base.database, dbname=name))
    with psycopg.connect(**settings.database.conninfo(), autocommit=True) as conn:
        conn.execute((PROJECT_ROOT / "sql" / "schema.sql").read_text())
    try:
        yield settings
    finally:
        with psycopg.connect(**admin.conninfo(), autocommit=True) as conn:
            conn.execute(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")


@pytest.fixture
def settings(database_settings: Settings, tmp_path: Path) -> Settings:
    return replace(
        database_settings,
        landing_dir=tmp_path / "landing",
        state_dir=tmp_path / "state",
        reports_dir=tmp_path / "reports",
    )


@pytest.fixture
def conn(settings: Settings):
    import psycopg

    with psycopg.connect(**settings.database.conninfo(), autocommit=True) as connection:
        for table in (
            "raw.customers",
            "raw.products",
            "raw.orders",
            "raw.payments",
            "raw.rejected_records",
            "audit.pipeline_runs",
            "audit.task_runs",
            "audit.ingestion_batches",
            "audit.quality_results",
        ):
            connection.execute(f"TRUNCATE {table}")
        yield connection
