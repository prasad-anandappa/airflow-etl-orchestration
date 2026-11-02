"""payments_daily_taskflow — the hand-written TaskFlow counterpart to the
factory DAGs.

The factory (dag_factory.py) is the right tool for N similar feeds; this
DAG exists for the flagship payments feed, where the extra steps —
schema-contract validation before the quality gate, and a transform step
(normalising currency codes and dropping voided rows) — don't generalise.
Keeping both styles in one repo is deliberate: it demonstrates knowing
when generation stops paying.

Semantics worth noting:
* ``catchup=False`` + a manual backfill DAG (backfill_payments.py) is the
  backfill strategy: scheduled runs never surprise-replay history; history
  is replayed explicitly, date by date, through the same code path.
* The quality gate sits *before* the load and quarantines on failure
  (operators/dq_gate.py); the load is idempotent per logical date
  (common/loaders.py), so a re-run after fixing the extract is safe.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

from common.alerting import on_failure_callback, on_sla_miss_callback
from common.loaders import Warehouse, read_csv_records
from operators.deferrable_file_sensor import DeferrableFileSensorOperator
from operators.dq_gate import DataQualityGateOperator

LANDING_DIR = os.environ.get("ETL_LANDING_DIR", "/tmp/etl/landing")
QUARANTINE_DIR = os.environ.get("ETL_QUARANTINE_DIR", "/tmp/etl/quarantine")
WAREHOUSE_PATH = os.environ.get("ETL_WAREHOUSE_PATH", "/tmp/etl/warehouse.db")

EXPECTED_COLUMNS = {"payment_id", "order_id", "amount", "currency", "status", "settled_at"}

CHECKS: list[dict[str, Any]] = [
    {"name": "row_count", "type": "row_count", "min_rows": 1},
    {"name": "unique_payment", "type": "unique", "columns": ["payment_id"]},
    {"name": "amount_present", "type": "not_null", "column": "amount"},
    {"name": "status_domain", "type": "values_in", "column": "status",
     "allowed": ["authorised", "settled", "failed", "voided"]},
]


@dag(
    dag_id="payments_daily_taskflow",
    description="Flagship payments ETL (TaskFlow): sense, validate contract, "
                "gate, transform, load, verify.",
    schedule="10 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": on_failure_callback,
    },
    sla_miss_callback=on_sla_miss_callback,
    tags=["etl", "payments", "taskflow"],
)
def payments_daily_taskflow() -> None:

    @task
    def validate_contract(ds: str | None = None) -> str:
        """Fail fast if the extract's shape drifted from the contract.

        Schema drift (renamed/dropped columns) is the most common silent
        breaker of downstream loads; catching it before the gate means the
        incident says "contract" instead of a cryptic load error.
        """
        path = Path(LANDING_DIR) / f"payments_{ds}.csv"
        records = read_csv_records(path)
        if not records:
            raise AirflowFailException(f"{path}: no records to validate")
        present = set(records[0])
        missing = EXPECTED_COLUMNS - present
        if missing:
            raise AirflowFailException(
                f"{path}: contract violation, missing columns {sorted(missing)}")
        return str(path)

    @task
    def transform(path: str) -> list[dict[str, Any]]:
        """Normalise: uppercase currency, drop voided rows, keep audit."""
        records = read_csv_records(path)
        out: list[dict[str, Any]] = []
        for r in records:
            if r.get("status") == "voided":
                continue
            r = dict(r)
            if r.get("currency"):
                r["currency"] = r["currency"].upper()
            out.append(r)
        return out

    @task
    def load(records: list[dict[str, Any]], ds: str | None = None) -> dict[str, Any]:
        warehouse = Warehouse(WAREHOUSE_PATH)
        try:
            return warehouse.load("payments", records, logical_date=ds or "",
                                  key_columns=["payment_id"])
        finally:
            warehouse.close()

    @task(sla=timedelta(minutes=30))
    def verify(load_result: dict[str, Any]) -> dict[str, Any]:
        warehouse = Warehouse(WAREHOUSE_PATH)
        try:
            return {"rows_in_table": warehouse.count("payments"),
                    "load": load_result}
        finally:
            warehouse.close()

    wait = DeferrableFileSensorOperator(
        task_id="wait_for_extract",
        filepath=f"{LANDING_DIR}/payments_{{{{ ds }}}}.csv",
        poll_interval=30,
    )
    gate = DataQualityGateOperator(
        task_id="quality_gate",
        filepath=f"{LANDING_DIR}/payments_{{{{ ds }}}}.csv",
        checks=CHECKS,
        quarantine_dir=QUARANTINE_DIR,
    )
    contract_ok = validate_contract()
    wait >> contract_ok >> gate
    verify(load(transform(contract_ok)))


payments_daily_taskflow()
