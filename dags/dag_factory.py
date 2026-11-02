"""Dynamic DAG generation: one task graph, N pipelines, config-driven.

Why generate DAGs from config instead of copy-pasting DAG files?

* The graph (sense -> stage -> gate -> load -> verify) is a *platform
  invariant*; only names, files, keys and checks vary. Copy-pasted DAGs
  drift — a fix lands in one pipeline and not its siblings. Here a fix
  lands once, in build_pipeline_dag().
* Adding a feed is a config change reviewable in a diff, not new code.

The failure mode of generation is opacity (a config typo becomes a broken
DAG at parse time), so config is validated eagerly in this module and the
DagBag tests in CI assert every expected dag_id exists with zero import
errors. Parse-time work is kept to YAML + validation only — no I/O against
external systems at parse time, per Airflow's own guidance.

Versioning: the factory and config are versioned together in git; a
generated DAG's code *is* this file at this commit. DAG ids are stable
("etl_<name>") so renaming a config entry is treated as retiring one DAG
and creating another — history does not silently transfer.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import yaml
from airflow import DAG
from airflow.operators.python import PythonOperator

from common.alerting import on_failure_callback, on_sla_miss_callback
from common.loaders import Warehouse, read_csv_records
from operators.deferrable_file_sensor import DeferrableFileSensorOperator
from operators.dq_gate import DataQualityGateOperator

DAGS_DIR = Path(__file__).parent
CONFIG_PATH = DAGS_DIR / "pipeline_config.yml"
LANDING_DIR = os.environ.get("ETL_LANDING_DIR", "/tmp/etl/landing")
QUARANTINE_DIR = os.environ.get("ETL_QUARANTINE_DIR", "/tmp/etl/quarantine")
WAREHOUSE_PATH = os.environ.get("ETL_WAREHOUSE_PATH", "/tmp/etl/warehouse.db")

REQUIRED_FIELDS = {"name", "file_pattern", "table", "key_columns", "schedule", "checks"}


def load_config(path: Path = CONFIG_PATH) -> list[dict[str, Any]]:
    """Load and validate pipeline definitions; raise on any defect."""
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    pipelines = raw.get("pipelines")
    if not isinstance(pipelines, list) or not pipelines:
        raise ValueError("pipeline_config.yml: 'pipelines' must be a non-empty list")
    seen: set[str] = set()
    for i, entry in enumerate(pipelines):
        missing = REQUIRED_FIELDS - set(entry)
        if missing:
            raise ValueError(f"pipelines[{i}] missing fields: {sorted(missing)}")
        if entry["name"] in seen:
            raise ValueError(f"duplicate pipeline name: {entry['name']}")
        seen.add(entry["name"])
        if not entry["key_columns"]:
            raise ValueError(f"pipeline {entry['name']}: key_columns must be non-empty")
        if not entry["checks"]:
            raise ValueError(f"pipeline {entry['name']}: at least one quality check required")
    return pipelines


def _load_callable(table: str, key_columns: list[str], file_pattern: str):
    def _load(ds: str | None = None, **_: Any) -> dict[str, Any]:
        filename = file_pattern.replace("{{ ds }}", ds or "")
        path = Path(LANDING_DIR) / filename
        records = read_csv_records(path)
        warehouse = Warehouse(WAREHOUSE_PATH)
        try:
            return warehouse.load(table, records, logical_date=ds or "",
                                  key_columns=key_columns)
        finally:
            warehouse.close()

    return _load


def _verify_callable(table: str):
    def _verify(ti: Any = None, **_: Any) -> dict[str, Any]:
        load_result = ti.xcom_pull(task_ids="load")
        warehouse = Warehouse(WAREHOUSE_PATH)
        try:
            count = warehouse.count(table)
        finally:
            warehouse.close()
        return {"table": table, "rows_in_table": count, "load": load_result}

    return _verify


def build_pipeline_dag(config: dict[str, Any]) -> DAG:
    dag_id = f"etl_{config['name']}"
    default_args = {
        "owner": "data-platform",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": on_failure_callback,
    }
    with DAG(
        dag_id=dag_id,
        description=f"Config-generated ETL for {config['name']} "
                    "(see dags/dag_factory.py)",
        schedule=config["schedule"],
        start_date=datetime(2026, 1, 1),
        catchup=False,
        max_active_runs=1,
        default_args=default_args,
        sla_miss_callback=on_sla_miss_callback,
        tags=["etl", "generated", config["name"]],
    ) as dag:
        filename = config["file_pattern"]
        sense = DeferrableFileSensorOperator(
            task_id="wait_for_extract",
            filepath=f"{LANDING_DIR}/{filename}",
            poll_interval=30,
        )
        gate = DataQualityGateOperator(
            task_id="quality_gate",
            filepath=f"{LANDING_DIR}/{filename}",
            checks=config["checks"],
            quarantine_dir=QUARANTINE_DIR,
            sla=timedelta(hours=1),
        )
        load = PythonOperator(
            task_id="load",
            python_callable=_load_callable(
                config["table"], config["key_columns"], config["file_pattern"]),
        )
        verify = PythonOperator(
            task_id="verify",
            python_callable=_verify_callable(config["table"]),
        )
        sense >> gate >> load >> verify
    return dag


# Airflow discovers DAG objects in module globals.
for _config in load_config():
    globals()[f"etl_{_config['name']}"] = build_pipeline_dag(_config)
