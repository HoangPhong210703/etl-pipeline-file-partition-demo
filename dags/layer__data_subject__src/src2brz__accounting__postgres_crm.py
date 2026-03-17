from datetime import datetime

from airflow import DAG #type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator #type: ignore

with DAG(
    dag_id="src2brz_accounting_crm",
    description="Button: trigger source-to-bronze for accounting from crm",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["button", "src2brz", "accounting", "postgres_crm"],
) as dag:

    trigger_coor = TriggerDagRunOperator(
        task_id="trigger_coordinator",
        trigger_dag_id="coordinator",
        conf={"data_subject": "accounting", "source": "postgres_crm"},
        wait_for_completion=False,
    )
