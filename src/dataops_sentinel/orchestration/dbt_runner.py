"""Run dbt as a subprocess and translate its exit status into pipeline errors."""

from __future__ import annotations

import logging
import os
import subprocess
from collections.abc import Callable, Sequence
from datetime import date
from typing import Any

from dataops_sentinel.config import Settings
from dataops_sentinel.errors import DataQualityError, PermanentPipelineError, TransientPipelineError

logger = logging.getLogger(__name__)

CONNECTION_ERROR_MARKERS = (
    "could not connect to server",
    "connection refused",
    "timeout expired",
    "the database system is starting up",
)


def build_command(settings: Settings, command: str = "build") -> list[str]:
    return [
        settings.dbt_executable,
        command,
        "--project-dir",
        str(settings.dbt_project_dir),
        "--profiles-dir",
        str(settings.dbt_project_dir),
    ]


def dbt_environment(settings: Settings) -> dict[str, str]:
    env = dict(os.environ)
    db = settings.database
    env.update(
        {
            "SENTINEL_DB_HOST": db.host,
            "SENTINEL_DB_PORT": str(db.port),
            "SENTINEL_DB_NAME": db.dbname,
            "SENTINEL_DB_USER": db.user,
            "SENTINEL_DB_PASSWORD": db.password,
        }
    )
    return env


def interpret(returncode: int, output: str) -> None:
    if returncode == 0:
        return
    lowered = output.lower()
    if any(marker in lowered for marker in CONNECTION_ERROR_MARKERS):
        raise TransientPipelineError("dbt could not reach the database")
    failed = [line.strip() for line in output.splitlines() if "failure in test" in line.lower()]
    if failed:
        raise DataQualityError("dbt tests failed", failed)
    raise PermanentPipelineError(f"dbt exited with status {returncode}")


def run_dbt(
    settings: Settings,
    logical_date: date,
    command: str = "build",
    runner: Callable[..., Any] = subprocess.run,
    timeout_seconds: int = 900,
) -> str:
    args: Sequence[str] = build_command(settings, command)
    logger.info("Running dbt", extra={"command": " ".join(args), "logical_date": str(logical_date)})
    try:
        completed = runner(
            list(args),
            capture_output=True,
            text=True,
            env=dbt_environment(settings),
            timeout=timeout_seconds,
            check=False,
        )
    except FileNotFoundError as exc:
        raise PermanentPipelineError(
            f"dbt executable not found: {settings.dbt_executable}"
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise TransientPipelineError(f"dbt timed out after {timeout_seconds}s") from exc
    output = (completed.stdout or "") + (completed.stderr or "")
    for line in output.splitlines()[-40:]:
        logger.info("dbt | %s", line)
    interpret(completed.returncode, output)
    return output
