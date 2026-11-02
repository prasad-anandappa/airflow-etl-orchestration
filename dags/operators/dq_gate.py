"""DataQualityGateOperator — runs the shared check suite as a hard gate.

Failure routing is part of the operator, not an afterthought in a callback:

* The failing extract is **moved to the quarantine directory** (never
  deleted — an operator needs the evidence, and a fixed re-delivery must
  be diffable against what failed).
* A JSON quality report is written next to it (per-check results), so the
  incident is diagnosable without opening Airflow logs.
* Only then does the task fail, which triggers the DAG's alerting hooks.

A gate that merely logs and passes is a dashboard, not a gate.
"""

from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from airflow.exceptions import AirflowFailException
from airflow.models import BaseOperator

from common.loaders import read_csv_records
from common.quality import run_checks


class DataQualityGateOperator(BaseOperator):
    """Validate a staged CSV extract against a list of check definitions."""

    template_fields = ("filepath",)

    def __init__(
        self,
        *,
        filepath: str,
        checks: list[dict[str, Any]],
        quarantine_dir: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.filepath = filepath
        self.checks = checks
        self.quarantine_dir = quarantine_dir

    def execute(self, context: dict[str, Any]) -> dict[str, Any]:
        path = Path(self.filepath)
        if not path.exists():
            raise AirflowFailException(f"extract not found: {path}")
        records = read_csv_records(path)
        outcome = run_checks(records, self.checks)
        self.log.info("quality results:\n%s", outcome.summary())
        for warning in outcome.warnings:
            self.log.warning("quality warning: %s — %s", warning.name, warning.detail)

        report = {
            "file": str(path),
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "dag_id": context["dag"].dag_id,
            "run_id": context["run_id"],
            "results": [
                {"name": r.name, "passed": r.passed,
                 "severity": r.severity, "detail": r.detail}
                for r in outcome.results
            ],
        }
        if not outcome.passed:
            if self.quarantine_dir:
                dest_dir = Path(self.quarantine_dir)
                dest_dir.mkdir(parents=True, exist_ok=True)
                moved = dest_dir / path.name
                shutil.move(str(path), moved)
                (dest_dir / f"{path.name}.quality.json").write_text(
                    json.dumps(report, indent=2), encoding="utf-8")
                self.log.error("extract quarantined to %s", moved)
            raise AirflowFailException(
                f"data-quality gate failed for {path}:\n{outcome.summary()}")
        return {"records": len(records), "checks": len(outcome.results),
                "warnings": len(outcome.warnings)}
