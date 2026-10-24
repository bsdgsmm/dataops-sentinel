"""Runtime settings read from environment variables."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


class ConfigError(ValueError):
    """Raised when an environment variable has an invalid value."""


def _text(env: Mapping[str, str], name: str, default: str) -> str:
    value = env.get(name, "").strip()
    return value or default


def _number(env: Mapping[str, str], name: str, default: float, minimum: float = 0) -> float:
    raw = env.get(name, "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be a number, got {raw!r}") from exc
    if value < minimum:
        raise ConfigError(f"{name} must be >= {minimum}, got {value}")
    return value


@dataclass(frozen=True)
class DatabaseSettings:
    host: str = "localhost"
    port: int = 5432
    dbname: str = "warehouse"
    user: str = "sentinel"
    password: str = field(default="", repr=False)
    connect_timeout_seconds: int = 10

    def conninfo(self) -> dict[str, object]:
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.dbname,
            "user": self.user,
            "password": self.password,
            "connect_timeout": self.connect_timeout_seconds,
            "application_name": "dataops-sentinel",
        }


@dataclass(frozen=True)
class QualityThresholds:
    reject_rate_warn: float = 0.01
    reject_rate_fail: float = 0.05
    orphan_rate_warn: float = 0.0
    orphan_rate_fail: float = 0.05
    row_count_change_warn: float = 0.5
    revenue_diff_warn: float = 0.01
    revenue_diff_fail: float = 0.10


@dataclass(frozen=True)
class Settings:
    database: DatabaseSettings
    thresholds: QualityThresholds
    landing_dir: Path
    reports_dir: Path
    state_dir: Path
    dbt_project_dir: Path
    dbt_executable: str
    schema_path: Path
    fault_injection: str
    log_level: str

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Settings:
        env = os.environ if env is None else env
        defaults = QualityThresholds()
        database = DatabaseSettings(
            host=_text(env, "SENTINEL_DB_HOST", "localhost"),
            port=int(_number(env, "SENTINEL_DB_PORT", 5432, minimum=1)),
            dbname=_text(env, "SENTINEL_DB_NAME", "warehouse"),
            user=_text(env, "SENTINEL_DB_USER", "sentinel"),
            password=env.get("SENTINEL_DB_PASSWORD", ""),
            connect_timeout_seconds=int(_number(env, "SENTINEL_DB_CONNECT_TIMEOUT", 10, 1)),
        )
        thresholds = QualityThresholds(
            reject_rate_warn=_number(env, "SENTINEL_REJECT_RATE_WARN", defaults.reject_rate_warn),
            reject_rate_fail=_number(env, "SENTINEL_REJECT_RATE_FAIL", defaults.reject_rate_fail),
            orphan_rate_warn=_number(env, "SENTINEL_ORPHAN_RATE_WARN", defaults.orphan_rate_warn),
            orphan_rate_fail=_number(env, "SENTINEL_ORPHAN_RATE_FAIL", defaults.orphan_rate_fail),
            row_count_change_warn=_number(
                env, "SENTINEL_ROW_COUNT_CHANGE_WARN", defaults.row_count_change_warn
            ),
            revenue_diff_warn=_number(
                env, "SENTINEL_REVENUE_DIFF_WARN", defaults.revenue_diff_warn
            ),
            revenue_diff_fail=_number(
                env, "SENTINEL_REVENUE_DIFF_FAIL", defaults.revenue_diff_fail
            ),
        )
        if thresholds.reject_rate_warn > thresholds.reject_rate_fail:
            raise ConfigError("SENTINEL_REJECT_RATE_WARN must not exceed SENTINEL_REJECT_RATE_FAIL")
        return cls(
            database=database,
            thresholds=thresholds,
            landing_dir=Path(
                _text(env, "SENTINEL_LANDING_DIR", str(PROJECT_ROOT / "data/landing"))
            ),
            reports_dir=Path(_text(env, "SENTINEL_REPORTS_DIR", str(PROJECT_ROOT / "reports"))),
            state_dir=Path(_text(env, "SENTINEL_STATE_DIR", str(PROJECT_ROOT / "data/state"))),
            dbt_project_dir=Path(_text(env, "SENTINEL_DBT_PROJECT_DIR", str(PROJECT_ROOT / "dbt"))),
            dbt_executable=_text(env, "SENTINEL_DBT_EXECUTABLE", "dbt"),
            schema_path=Path(
                _text(env, "SENTINEL_SCHEMA_PATH", str(PROJECT_ROOT / "sql" / "schema.sql"))
            ),
            fault_injection=env.get("SENTINEL_FAULT_INJECTION", "").strip(),
            log_level=_text(env, "SENTINEL_LOG_LEVEL", "INFO").upper(),
        )
