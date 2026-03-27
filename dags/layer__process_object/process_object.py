"""Unified process_object DAG — receives config for a single (data_subject, source) pair,
filters active tables, sorts by load_sequence, and triggers the execution DAG for the layer."""

import sys
from datetime import datetime

from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # type: ignore

sys.path.insert(0, "/opt/airflow")
from src.pipeline.audit import audited


EXECUTION_DAGS = {
    "src2brz": "src2brz_rdbms2parquet_ingestion",
    "brz2sil": "brz2stg_parquet2postgres_ingestion",
}


@audited
def process_object(**kwargs):
    """Filter active tables, sort by load_sequence, and prepare payload for ingestion."""
    dag_run = kwargs["dag_run"]
    conf = dag_run.conf or {}

    layer = conf.get("layer")
    data_subject = conf.get("data_subject")
    source = conf.get("source")
    tables = conf.get("tables", [])

    tables_active = [t for t in tables if t.get("table_load_active", True)]
    tables_sorted = sorted(tables_active, key=lambda t: int(t.get("load_sequence", 0)))

    execution_dag = EXECUTION_DAGS.get(layer)
    if not execution_dag:
        raise ValueError(f"No execution DAG mapped for layer '{layer}'")

    print(f"[process_object] layer={layer}, data_subject={data_subject}, source={source}")
    print(f"[process_object] Tables: {len(tables)} total, {len(tables_active)} active")
    for t in tables_sorted:
        print(f"  seq={t['load_sequence']} {t['table_name']}")

    return {
        "button": conf.get("button"),
        "layer": layer,
        "data_subject": data_subject,
        "source": source,
        "tables": tables_sorted,
        "execution_dag": execution_dag,
    }


with DAG(
    dag_id="process_object",
    description="Filter active tables, sort by load_sequence, and trigger execution DAG",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["orchestration"],
) as dag:
    process_object_task = PythonOperator(
        task_id="process_object",
        python_callable=process_object,
    )

    ingest_trigger = TriggerDagRunOperator(
        task_id="ingest_trigger",
        trigger_dag_id="{{ ti.xcom_pull(task_ids='process_object')['execution_dag'] }}",
        conf="{{ ti.xcom_pull(task_ids='process_object') }}",
    )

    process_object_task >> ingest_trigger
