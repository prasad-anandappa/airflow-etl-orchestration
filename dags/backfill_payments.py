"""backfill_payments — explicit, bounded history replay.

Strategy (documented because "just turn on catchup" is how pipelines get
hurt): the daily DAGs run with ``catchup=False`` so a scheduler outage or
a new deployment never triggers an uncontrolled replay storm. When history
must be (re)processed — a fixed transform, a recovered upstream — an
operator triggers THIS DAG with a bounded date range in the run conf:

    airflow dags trigger backfill_payments \
        --conf '{"start": "2026-01-01", "end": "2026-01-31"}'

Each date is processed by a dynamically mapped task running the *same*
load function as the daily DAG, keyed by that logical date, so the
idempotent loader makes the whole backfill re-runnable: trigger it twice
and the second run verifies + no-ops per date. ``max_active_runs=1`` and
the mapped parallelism cap keep the replay from starving the daily runs.
"""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

from common.alerting import on_failure_callback
from common.loaders import Warehouse, read_csv_records

LANDING_DIR = os.environ.get("ETL_LANDING_DIR", "/tmp/etl/landing")
WAREHOUSE_PATH = os.environ.get("ETL_WAREHOUSE_PATH", "/tmp/etl/warehouse.db")
MAX_BACKFILL_DAYS = 92  # one quarter — anything bigger needs a plan, not a conf


def date_range_from_conf(conf: dict[str, Any] | None) -> list[str]:
    """Expand and validate the trigger conf into a list of ISO dates."""
    if not conf or "start" not in conf or "end" not in conf:
        raise AirflowFailException(
            'backfill requires dag_run.conf {"start": "YYYY-MM-DD", "end": "YYYY-MM-DD"}')
    start = date.fromisoformat(conf["start"])
    end = date.fromisoformat(conf["end"])
    if end < start:
        raise AirflowFailException("conf end is before start")
    days = (end - start).days + 1
    if days > MAX_BACKFILL_DAYS:
        raise AirflowFailException(
            f"backfill of {days} days exceeds the {MAX_BACKFILL_DAYS}-day "
            "safety bound; split the range")
    return [(start + timedelta(days=i)).isoformat() for i in range(days)]


@dag(
    dag_id="backfill_payments",
    description="Manual, bounded backfill of the payments feed by date range.",
    schedule=None,  # trigger-only, deliberately
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": on_failure_callback,
    },
    tags=["etl", "payments", "backfill"],
)
def backfill_payments() -> None:

    @task
    def plan(dag_run: Any = None) -> list[str]:
        conf = dag_run.conf if dag_run else None
        dates = date_range_from_conf(conf)
        return dates

    @task(max_active_tis_per_dag=4)
    def load_one(target_date: str) -> dict[str, Any]:
        # NB: the parameter is named target_date, not logical_date —
        # logical_date is a reserved Airflow context variable and TaskFlow
        # refuses to expand() over it.
        path = Path(LANDING_DIR) / f"payments_{target_date}.csv"
        records = read_csv_records(path)
        warehouse = Warehouse(WAREHOUSE_PATH)
        try:
            return warehouse.load("payments", records,
                                  logical_date=target_date,
                                  key_columns=["payment_id"])
        finally:
            warehouse.close()

    @task
    def summarise(results: list[dict[str, Any]]) -> dict[str, Any]:
        loaded = sum(1 for r in results if r["status"] == "loaded")
        skipped = sum(1 for r in results if r["status"] == "skipped_duplicate")
        return {"days_loaded": loaded, "days_skipped_duplicate": skipped,
                "rows_written": sum(r["rows"] for r in results)}

    dates = plan()
    summarise(load_one.expand(target_date=dates))


backfill_payments()
