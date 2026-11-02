import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DAGS = ROOT / "dags"

# DAG modules import shared packages (common/, operators/) relative to the
# dags folder, exactly as Airflow resolves them at runtime.
sys.path.insert(0, str(DAGS))

os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("ETL_LANDING_DIR", "/tmp/etl-test/landing")
os.environ.setdefault("ETL_QUARANTINE_DIR", "/tmp/etl-test/quarantine")
os.environ.setdefault("ETL_WAREHOUSE_PATH", "/tmp/etl-test/warehouse.db")
