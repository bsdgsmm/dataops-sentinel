from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

FIXTURE_LANDING = Path(__file__).parent / "fixtures" / "landing"
FIXTURE_DATE = date(2026, 1, 2)


@pytest.fixture
def landing_dir() -> Path:
    return FIXTURE_LANDING
