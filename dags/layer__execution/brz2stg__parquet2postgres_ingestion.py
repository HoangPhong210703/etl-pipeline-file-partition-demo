"""Execution DAG (brz2stg) — loads parquet files into postgres warehouse
for a single (data_subject, source) pair."""

import sys
from datetime import datetime

from airflow import DAG  # type: ignore
from airflow.datasets import Dataset, DatasetAlias  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore

sys.path.insert(0, "/opt/airflow")
from src.pipeline.audit import audited
from src.pipeline.alert import dag_failure_callback, dag_success_callback
from src.pipeline.settings import BRONZE_BASE_URL


@audited
def verify_parquet(**kwargs):
    """Check that parquet files exist for the tables in this (data_subject, source) pair."""
    from src.pipeline.staging import get_parquet_dir, get_latest_parquet_file

    conf = kwargs["dag_run"].conf or {}
    source = conf["source"]
    data_subject = conf["data_subject"]
    tables = conf.get("tables", [])

    found = 0
    missing = 0
    for t in tables:
        parquet_dir = get_parquet_dir(
            bronze_base_url=BRONZE_BASE_URL,
            data_subject=data_subject,
            source_name=source,
            schema=t["source_schema"],
            table_name=t["table_name"],
        )
        latest = get_latest_parquet_file(parquet_dir)
        if latest:
            found += 1
        else:
            missing += 1
            print(f"[verify_parquet] No parquet found: {parquet_dir}")

    print(f"[verify_parquet] {source}__{data_subject}: {found} found, {missing} missing out of {len(tables)} tables")

    if found == 0:
        raise FileNotFoundError(f"No parquet files found for any table in {source}__{data_subject}")


@audited
def load_to_warehouse(**kwargs):
    """Load latest parquet files into warehouse."""
    from src.pipeline.staging import run_stg_subject, _print_stg_summary
    from src.pipeline.config import table_config_from_dict
    from src.pipeline.credentials import load_warehouse_credentials

    conf = kwargs["dag_run"].conf or {}
    source = conf["source"]
    data_subject = conf["data_subject"]
    tables = conf.get("tables", [])

    table_configs = [table_config_from_dict(t) for t in tables]
    source_schema = tables[0]["source_schema"] if tables else "public"
    warehouse_credentials = load_warehouse_credentials()

    results = run_stg_subject(
        source_name=source,
        source_schema=source_schema,
        data_subject=data_subject,
        tables=table_configs,
        bronze_base_url=BRONZE_BASE_URL,
        warehouse_credentials=warehouse_credentials,
    )

    _print_stg_summary(source, results)

    failed = [r for r in results if r[1] == "failed"]
    if failed:
        raise RuntimeError(f"{len(failed)} table(s) failed to load: {[r[0] for r in failed]}")

    # Emit dataset event so stg2sil knows this source's stg data is ready
    stg_alias = DatasetAlias("brz2stg-output")
    stg_dataset = Dataset(f"stg__{data_subject}__{source}")
    kwargs["outlet_events"][stg_alias].add(stg_dataset)
    print(f"[brz2stg] Emitted dataset event: stg__{data_subject}__{source}")

    total = sum(r[4] for r in results)
    return {"row_count": total}


with DAG(
    dag_id="brz2stg_parquet2postgres_ingestion",
    description="Load parquet files into postgres warehouse for a single (data_subject, source)",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    on_success_callback=dag_success_callback,
    on_failure_callback=dag_failure_callback,
    catchup=False,
    render_template_as_native_obj=True,
    tags=["ingestion", "brz2stg", "parquet2postgres"],
) as dag:
    verify = PythonOperator(task_id="verify_parquet", python_callable=verify_parquet)
    load = PythonOperator(task_id="load_to_warehouse", python_callable=load_to_warehouse, outlets=[DatasetAlias("brz2stg-output")])

    verify >> load
