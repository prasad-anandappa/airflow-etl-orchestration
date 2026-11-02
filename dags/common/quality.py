"""Data-quality check definitions — pure Python, unit-testable, DAG-agnostic.

Checks operate on ``list[dict]`` records so they can be tested without a
running Airflow (tests/test_quality_logic.py) and reused by any DAG. In the
DAGs they are executed by DataQualityGateOperator over the staged extract.

A check is a dict: {"name": ..., "type": ..., ...params}. The factory DAGs
read these from pipeline_config.yml; the explicit DAG defines them inline.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

Record = dict[str, Any]


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str
    severity: str = "error"  # "error" blocks the pipeline; "warn" only reports


@dataclass
class QualityOutcome:
    results: list[CheckResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(r.passed for r in self.results if r.severity == "error")

    @property
    def warnings(self) -> list[CheckResult]:
        return [r for r in self.results if r.severity == "warn" and not r.passed]

    def summary(self) -> str:
        lines = []
        for r in self.results:
            mark = "PASS" if r.passed else ("WARN-FAIL" if r.severity == "warn" else "FAIL")
            lines.append(f"[{mark}] {r.name}: {r.detail}")
        return "\n".join(lines)


def check_row_count(records: list[Record], *, min_rows: int = 1,
                    max_rows: int | None = None, **_: Any) -> CheckResult:
    n = len(records)
    ok = n >= min_rows and (max_rows is None or n <= max_rows)
    return CheckResult("row_count", ok,
                       f"{n} rows (expected >= {min_rows}"
                       + (f", <= {max_rows}" if max_rows else "") + ")")


def check_unique(records: list[Record], *, columns: list[str], **_: Any) -> CheckResult:
    seen: set[tuple[Any, ...]] = set()
    dupes = 0
    for r in records:
        key = tuple(r.get(c) for c in columns)
        if key in seen:
            dupes += 1
        seen.add(key)
    return CheckResult(f"unique({','.join(columns)})", dupes == 0,
                       f"{dupes} duplicate key(s) over {len(records)} rows")


def check_not_null(records: list[Record], *, column: str, **_: Any) -> CheckResult:
    bad = sum(1 for r in records if r.get(column) in (None, ""))
    return CheckResult(f"not_null({column})", bad == 0,
                       f"{bad} null/empty value(s) in {column}")


def check_values_in(records: list[Record], *, column: str,
                    allowed: list[Any], **_: Any) -> CheckResult:
    bad_values = sorted({r.get(column) for r in records
                         if r.get(column) not in set(allowed)})
    return CheckResult(f"values_in({column})", not bad_values,
                       f"disallowed values: {bad_values[:5]}" if bad_values
                       else "all values allowed")


def check_freshness(records: list[Record], *, column: str,
                    max_age_hours: float = 36.0, **_: Any) -> CheckResult:
    stamps: list[datetime] = []
    for r in records:
        raw = r.get(column)
        if isinstance(raw, str) and raw:
            try:
                stamps.append(datetime.fromisoformat(raw))
            except ValueError:
                pass
    if not stamps:
        return CheckResult(f"freshness({column})", False,
                           f"no parseable timestamps in {column}")
    newest = max(s.replace(tzinfo=s.tzinfo or timezone.utc) for s in stamps)
    age_hours = (datetime.now(timezone.utc) - newest).total_seconds() / 3600
    return CheckResult(f"freshness({column})", age_hours <= max_age_hours,
                       f"newest {column} is {age_hours:.1f}h old "
                       f"(limit {max_age_hours}h)")


CHECK_FUNCS = {
    "row_count": check_row_count,
    "unique": check_unique,
    "not_null": check_not_null,
    "values_in": check_values_in,
    "freshness": check_freshness,
}


def run_checks(records: list[Record],
               definitions: list[dict[str, Any]]) -> QualityOutcome:
    """Execute check definition dicts against records, in order."""
    outcome = QualityOutcome()
    for definition in definitions:
        params = dict(definition)
        check_type = params.pop("type")
        name = params.pop("name", check_type)
        severity = params.pop("severity", "error")
        if check_type not in CHECK_FUNCS:
            outcome.results.append(
                CheckResult(name, False, f"unknown check type: {check_type}"))
            continue
        result = CHECK_FUNCS[check_type](records, **params)
        result.name = name
        result.severity = severity
        outcome.results.append(result)
    return outcome
