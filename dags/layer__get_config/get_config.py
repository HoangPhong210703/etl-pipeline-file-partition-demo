"""Unified get_config DAG — reads table_config.csv for a specific
(layer, data_subject, source) triplet and triggers processing."""

import sys
from datetime import datetime

from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # type: ignore

sys.path.insert(0, "/opt/airflow")
from src.pipeline.audit import audited


@audited
def get_config(**kwargs):
    """Read table_config.csv, filter by data_subject + source from coordinator."""
    from src.pipeline.config import load_csv_config, csv_table_config_to_dict
    from src.pipeline.settings import TABLE_CONFIG_PATH

    dag_run = kwargs["dag_run"]
    conf = dag_run.conf or {}
    layer = conf.get("layer")
    data_subject = conf.get("data_subject")
    source = conf.get("source")

    if not TABLE_CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config file not found: {TABLE_CONFIG_PATH}")

    all_configs = load_csv_config(TABLE_CONFIG_PATH)

    filtered = all_configs
    if data_subject:
        filtered = [c for c in filtered if c.data_subject == data_subject]
    if source:
        filtered = [c for c in filtered if c.source_name == source]

    tables = [csv_table_config_to_dict(c) for c in filtered]

    print(f"[get_config] layer={layer}, data_subject={data_subject}, source={source}")
    print(f"[get_config] Tables found: {len(tables)}")

    return {
        "button": conf.get("button"),
        "layer": layer,
        "data_subject": data_subject,
        "source": source,
        "tables": tables,
    }


with DAG(
    dag_id="get_config",
    description="Read table config for a (data_subject, source) pair and trigger processing",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["orchestration"],
) as dag:
    get_config_task = PythonOperator(
        task_id="get_config",
        python_callable=get_config,
    )

    processing_trigger = TriggerDagRunOperator(
        task_id="processing_trigger",
        trigger_dag_id="process_object",
        conf="{{ ti.xcom_pull(task_ids='get_config') }}",
    )

    get_config_task >> processing_trigger
