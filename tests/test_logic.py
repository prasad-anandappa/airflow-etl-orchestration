"""Unit tests for the pure-Python parts: quality checks, the warehouse
loader, config validation, and the backfill date-range planner.

These run without a scheduler — the DAGs are thin orchestration over
these functions, so most behaviour is provable here.
"""

from datetime import datetime, timedelta, timezone

import pytest

from common.loaders import Warehouse
from common.quality import run_checks
from dag_factory import REQUIRED_FIELDS, load_config
from backfill_payments import date_range_from_conf

ROWS = [
    {"payment_id": "p1", "amount": "10.00", "status": "settled"},
    {"payment_id": "p2", "amount": "", "status": "pending"},
    {"payment_id": "p2", "amount": "5.00", "status": "settled"},
]


def outcome_for(*defs):
    return run_checks(ROWS, list(defs))


def test_row_count_and_unique():
    outcome = outcome_for(
        {"name": "rc", "type": "row_count", "min_rows": 1},
        {"name": "uq", "type": "unique", "columns": ["payment_id"]},
    )
    assert outcome.results[0].passed
    assert not outcome.results[1].passed
    assert not outcome.passed  # error severity blocks


def test_not_null_and_warn_severity():
    outcome = outcome_for(
        {"name": "nn", "type": "not_null", "column": "amount", "severity": "warn"},
    )
    assert outcome.passed  # warn failures report but don't block
    assert len(outcome.warnings) == 1


def test_values_in_and_unknown_check_type():
    outcome = outcome_for(
        {"name": "vi", "type": "values_in", "column": "status",
         "allowed": ["settled"]},
        {"name": "mystery", "type": "does_not_exist"},
    )
    assert not outcome.results[0].passed
    assert not outcome.results[1].passed  # unknown types fail closed


def test_freshness():
    fresh = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    rows = [{"ts": fresh}]
    outcome = run_checks(rows, [{"name": "f", "type": "freshness",
                                 "column": "ts", "max_age_hours": 36}])
    assert outcome.passed
    stale = run_checks([{"ts": "2020-01-01T00:00:00+00:00"}],
                       [{"name": "f", "type": "freshness", "column": "ts"}])
    assert not stale.passed


def test_warehouse_idempotent_load(tmp_path):
    wh = Warehouse(tmp_path / "w.db")
    rows = [{"payment_id": "p1", "amount": "10"}, {"payment_id": "p2", "amount": "3"}]
    first = wh.load("payments", rows, logical_date="2026-01-01",
                    key_columns=["payment_id"])
    second = wh.load("payments", rows, logical_date="2026-01-01",
                     key_columns=["payment_id"])
    assert first["status"] == "loaded"
    assert second["status"] == "skipped_duplicate"
    assert wh.count("payments") == 2
    wh.close()


def test_warehouse_rejects_changed_batch(tmp_path):
    wh = Warehouse(tmp_path / "w.db")
    wh.load("payments", [{"payment_id": "p1", "amount": "10"}],
            logical_date="2026-01-01", key_columns=["payment_id"])
    with pytest.raises(ValueError, match="different contents"):
        wh.load("payments", [{"payment_id": "p1", "amount": "99"}],
                logical_date="2026-01-01", key_columns=["payment_id"])
    wh.close()


def test_config_is_valid_and_complete():
    pipelines = load_config()
    names = {p["name"] for p in pipelines}
    assert names == {"payments", "refunds", "customers"}
    for p in pipelines:
        assert REQUIRED_FIELDS <= set(p)


def test_backfill_range_expansion_and_bounds():
    dates = date_range_from_conf({"start": "2026-01-01", "end": "2026-01-03"})
    assert dates == ["2026-01-01", "2026-01-02", "2026-01-03"]
    with pytest.raises(Exception):
        date_range_from_conf({"start": "2026-02-01", "end": "2026-01-01"})
    with pytest.raises(Exception):  # over the 92-day safety bound
        date_range_from_conf({"start": "2026-01-01", "end": "2026-12-31"})
    with pytest.raises(Exception):
        date_range_from_conf(None)
