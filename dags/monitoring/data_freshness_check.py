"""Monitoring DAG — checks data freshness and alerts on stale sources.
Runs daily at 8 AM UTC. Reads freshness_config.csv for thresholds."""

import sys
from datetime import datetime

from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore

sys.path.insert(0, "/opt/airflow")
from src.pipeline.audit import audited


@audited
def run_freshness_check(**kwargs):
    """Delegate to src.pipeline.freshness — all logic lives there."""
    from src.pipeline.freshness import check_freshness
    check_freshness()


with DAG(
    dag_id="data_freshness_check",
    description="Check data freshness and alert on stale sources",
    schedule="0 8 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["monitoring", "freshness"],
) as dag:
    check = PythonOperator(
        task_id="check_freshness",
        python_callable=run_freshness_check,
    )
