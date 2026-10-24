"""Read a landing CSV into raw string rows after checking its header."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from dataops_sentinel.errors import SourceFileError
from dataops_sentinel.ingestion.schemas import DatasetSpec


@dataclass(frozen=True)
class SourceFile:
    spec: DatasetSpec
    logical_date: date
    path: Path
    header: tuple[str, ...]
    rows: list[tuple[int, dict[str, str]]]

    @property
    def unexpected_columns(self) -> tuple[str, ...]:
        return tuple(name for name in self.header if name not in self.spec.column_names)


def landing_path(landing_dir: Path, logical_date: date, spec: DatasetSpec) -> Path:
    return landing_dir / logical_date.isoformat() / spec.file_name


def read_source_file(landing_dir: Path, logical_date: date, spec: DatasetSpec) -> SourceFile:
    """Read one dataset file.

    Row numbers are 1-based and count data lines after the header. A missing
    file or a missing required column is a ``SourceFileError``.
    """
    path = landing_path(landing_dir, logical_date, spec)
    if not path.is_file():
        raise SourceFileError(f"{spec.name}: landing file not found: {path}")
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise SourceFileError(f"{spec.name}: file is empty: {path}")
        header = tuple(name.strip() for name in reader.fieldnames)
        missing = [name for name in spec.column_names if name not in header]
        if missing:
            raise SourceFileError(f"{spec.name}: missing required columns {missing} in {path}")
        rows: list[tuple[int, dict[str, str]]] = []
        for number, record in enumerate(reader, start=1):
            if None in record:
                record = {key: value for key, value in record.items() if key is not None}
                record["__extra_values__"] = "1"
            rows.append(
                (
                    number,
                    {(key or "").strip(): (value or "").strip() for key, value in record.items()},
                )
            )
    return SourceFile(spec, logical_date, path, header, rows)
