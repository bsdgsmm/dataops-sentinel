"""Opt-in fault injection for local recovery demonstrations.

``SENTINEL_FAULT_INJECTION`` holds comma-separated rules of the form
``<task>@<YYYY-MM-DD>:<mode>``:

* ``transient_once`` - the first attempt of that task for that date raises a
  ``TransientPipelineError`` just before its database commit, so all of its
  writes are rolled back; later attempts succeed. A marker file in the state
  directory records that the fault already fired.
* ``always`` - every attempt fails, to show what an exhausted retry looks like.

Nothing is injected when the variable is empty, which is the default.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from dataops_sentinel.errors import TransientPipelineError

logger = logging.getLogger(__name__)

MODES = ("transient_once", "always")


@dataclass(frozen=True)
class FaultRule:
    task: str
    logical_date: date
    mode: str


def parse_rules(spec: str) -> list[FaultRule]:
    rules = []
    for item in filter(None, (part.strip() for part in spec.split(","))):
        try:
            target, mode = item.rsplit(":", 1)
            task, day = target.split("@", 1)
            rule = FaultRule(task.strip(), date.fromisoformat(day.strip()), mode.strip())
        except ValueError as exc:
            raise ValueError(
                f"invalid fault rule {item!r}; expected <task>@<YYYY-MM-DD>:<mode>"
            ) from exc
        if rule.mode not in MODES:
            raise ValueError(f"invalid fault mode {rule.mode!r}; choose one of {MODES}")
        rules.append(rule)
    return rules


class FaultInjector:
    def __init__(self, spec: str, state_dir: Path) -> None:
        self.rules = parse_rules(spec)
        self.state_dir = state_dir

    def _marker(self, rule: FaultRule) -> Path:
        return self.state_dir / f"fault-{rule.task}-{rule.logical_date.isoformat()}.fired"

    def maybe_fail(self, task: str, logical_date: date) -> None:
        for rule in self.rules:
            if rule.task != task or rule.logical_date != logical_date:
                continue
            if rule.mode == "always":
                raise TransientPipelineError(f"injected failure ({task} {logical_date}, always)")
            marker = self._marker(rule)
            if marker.exists():
                continue
            self.state_dir.mkdir(parents=True, exist_ok=True)
            marker.write_text("fired\n", encoding="utf-8")
            logger.warning(
                "Injecting transient failure", extra={"task": task, "date": str(logical_date)}
            )
            raise TransientPipelineError(f"injected failure ({task} {logical_date}, first attempt)")
