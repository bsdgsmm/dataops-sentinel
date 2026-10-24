"""Exception taxonomy used to decide whether a failed task should be retried."""

from __future__ import annotations


class SentinelError(Exception):
    """Base class for pipeline errors."""


class TransientPipelineError(SentinelError):
    """A temporary failure. Retrying the same task is expected to succeed."""


class PermanentPipelineError(SentinelError):
    """Retrying will not help until the input or configuration changes."""


class SourceFileError(PermanentPipelineError):
    """A landing file is missing or does not match the dataset contract."""


class DataQualityError(PermanentPipelineError):
    """At least one blocking data-quality check failed."""

    def __init__(self, message: str, failed_checks: list[str]) -> None:
        super().__init__(message)
        self.failed_checks = failed_checks
