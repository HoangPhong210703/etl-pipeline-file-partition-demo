# Full Refactor: Code Quality, Architecture, Config-Driven Consistency

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate code duplication across DAG layers, centralize hardcoded paths, and unify configuration files so that adding a new source/layer requires only CSV edits (no Python changes).

**Architecture:** Collapse the duplicated get_config and process_object DAGs (src2brz vs brz2sil) into single parameterized DAGs. Extract all hardcoded paths into one settings module. Merge the identical src2brz_config.csv and brz2sil_config.csv into a single table_config.csv. Remove dead code from bronze.py.

**Tech Stack:** Python 3.11, Apache Airflow 2.10.4, dlt, dbt, pytest

---

## File Structure

### New files to create:
- `src/ingestion/settings.py` — single source of truth for all paths and constants
- `dags/layer__get_config/get_config.py` — unified get_config DAG (replaces src2brz + brz2sil versions)
- `dags/layer__process_object/process_object.py` — unified process_object DAG (replaces src2brz + brz2sil versions)
- `config/table_config.csv` — unified table config (replaces src2brz_config.csv + brz2sil_config.csv)
- `tests/ingestion/test_settings.py` — tests for settings module

### Files to modify:
- `dags/coordinator.py` — update trigger target from `{layer}_get_config` to `get_config`
- `dags/layer__execution/src2brz__rdbms2parquet_ingestion.py` — use settings module
- `dags/layer__execution/brz2stg__parquet2postgres_ingestion.py` — use settings module
- `dags/layer__execution/stg2sil__process_whdata.py` — use settings module, config-driven dataset triggers
- `dags/monitoring/data_freshness_check.py` — use settings module
- `dags/monitoring/bronze_file_retention.py` — use settings module
- `src/ingestion/bronze.py` — use settings, remove dead functions
- `src/ingestion/stg.py` — use settings
- `src/ingestion/retention.py` — use settings
- `src/ingestion/alert.py` — use settings
- `src/ingestion/layer_management.py` — use settings
- `src/ingestion/audit/file_logger.py` — use settings
- `src/ingestion/audit/db_logger.py` — use settings

### Files to delete:
- `dags/layer__get_config/src2brz__get_config.py`
- `dags/layer__get_config/brz2sil__get_config.py`
- `dags/layer__process_object/src2brz__process_object.py`
- `dags/layer__process_object/brz2sil__process_object.py`
- `config/src2brz_config.csv` (replaced by table_config.csv)
- `config/brz2sil_config.csv` (replaced by table_config.csv)

---

### Task 1: Create centralized settings module

**Files:**
- Create: `src/ingestion/settings.py`
- Create: `tests/ingestion/test_settings.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/ingestion/test_settings.py
from pathlib import Path
from src.ingestion.settings import (
    SECRETS_PATH,
    BRONZE_BASE_URL,
    CONFIG_DIR,
    AUDIT_LOG_DIR,
    DBT_PROJECT_DIR,
)


def test_all_paths_are_path_objects():
    assert isinstance(SECRETS_PATH, Path)
    assert isinstance(CONFIG_DIR, Path)
    assert isinstance(AUDIT_LOG_DIR, Path)
    assert isinstance(DBT_PROJECT_DIR, Path)


def test_bronze_base_url_is_string():
    assert isinstance(BRONZE_BASE_URL, str)


def test_paths_are_absolute():
    assert SECRETS_PATH.is_absolute()
    assert CONFIG_DIR.is_absolute()
    assert AUDIT_LOG_DIR.is_absolute()
    assert DBT_PROJECT_DIR.is_absolute()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/ingestion/test_settings.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.ingestion.settings'`

- [ ] **Step 3: Write the settings module**

```python
# src/ingestion/settings.py
"""Centralized paths and constants — single source of truth for the entire project."""

import os
from pathlib import Path

# Base directory inside the Airflow container
AIRFLOW_HOME = Path(os.environ.get("AIRFLOW_HOME", "/opt/airflow"))

# Secrets
SECRETS_PATH = AIRFLOW_HOME / ".dlt" / "secrets.toml"

# Data paths
BRONZE_BASE_URL = str(AIRFLOW_HOME / "data" / "bronze")

# Config directory
CONFIG_DIR = AIRFLOW_HOME / "config"

# dbt
DBT_PROJECT_DIR = AIRFLOW_HOME / "dbt"

# Audit logs
AUDIT_LOG_DIR = AIRFLOW_HOME / "logs" / "audit"

# Config file paths (derived from CONFIG_DIR)
TABLE_CONFIG_PATH = CONFIG_DIR / "table_config.csv"
LAYER_MANAGEMENT_CONFIG_PATH = CONFIG_DIR / "layer_management_config.csv"
ALERT_CONFIG_PATH = CONFIG_DIR / "alert_config.csv"
FRESHNESS_CONFIG_PATH = CONFIG_DIR / "freshness_config.csv"
RETENTION_CONFIG_PATH = CONFIG_DIR / "retention_config.csv"
DAG_CONFIG_PATH = CONFIG_DIR / "dag_config.csv"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/ingestion/test_settings.py -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add src/ingestion/settings.py tests/ingestion/test_settings.py
git commit -m "refactor: add centralized settings module for all paths"
```

---

### Task 2: Unify table config CSVs

**Files:**
- Create: `config/table_config.csv`
- Delete (later): `config/src2brz_config.csv`, `config/brz2sil_config.csv`

The two config files are identical — merge them into one. Both layers read the same table definitions; the layer just determines what operation to perform.

- [ ] **Step 1: Create unified table_config.csv**

Copy `config/src2brz_config.csv` to `config/table_config.csv` (they are identical):

```bash
cp config/src2brz_config.csv config/table_config.csv
```

- [ ] **Step 2: Verify contents match**

```bash
diff config/src2brz_config.csv config/brz2sil_config.csv
```

Expected: No differences (files are identical).

- [ ] **Step 3: Commit**

```bash
git add config/table_config.csv
git commit -m "refactor: create unified table_config.csv (replaces src2brz + brz2sil configs)"
```

---

### Task 3: Collapse get_config DAGs into one parameterized DAG

**Files:**
- Create: `dags/layer__get_config/get_config.py`
- Modify: `dags/coordinator.py`
- Delete (later): `dags/layer__get_config/src2brz__get_config.py`, `dags/layer__get_config/brz2sil__get_config.py`

Currently `src2brz__get_config.py` and `brz2sil__get_config.py` are 94% identical. The only differences are:
1. The CONFIG_FILES dict key and path
2. The DAG ID
3. The trigger target DAG ID

With a unified table_config.csv, both can be replaced by one DAG that reads the layer from the conf.

- [ ] **Step 1: Create the unified get_config DAG**

```python
# dags/layer__get_config/get_config.py
"""Unified get_config DAG — reads table_config.csv for a specific
(layer, data_subject, source) triplet and triggers processing."""

import sys
from datetime import datetime

from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # type: ignore

sys.path.insert(0, "/opt/airflow")
from src.ingestion.audit import audited
from src.ingestion.settings import TABLE_CONFIG_PATH


PROCESS_OBJECT_DAGS = {
    "src2brz": "src2brz__process_object",
    "brz2sil": "brz2sil__process_object",
}


@audited
def get_config(**kwargs):
    """Read table_config.csv, filter by data_subject + source from coordinator."""
    from src.ingestion.config import load_csv_config

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

    tables = [
        {
            "id": c.id,
            "table_name": c.table_name,
            "source_name": c.source_name,
            "source_schema": c.source_schema,
            "data_subject": c.data_subject,
            "load_strategy": c.load_strategy,
            "cursor_column": c.cursor_column,
            "initial_value": c.initial_value,
            "primary_key": c.primary_key,
            "load_sequence": c.load_sequence,
            "table_load_active": c.table_load_active,
        }
        for c in filtered
    ]

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
        trigger_dag_id="{{ ti.xcom_pull(task_ids='get_config')['layer'] }}__process_object",
        conf="{{ ti.xcom_pull(task_ids='get_config') }}",
    )

    get_config_task >> processing_trigger
```

- [ ] **Step 2: Update coordinator to trigger unified get_config**

In `dags/coordinator.py`, change the `get_config_trigger` from:

```python
trigger_dag_id="{{ ti.xcom_pull(task_ids='coor')['layer'] }}_get_config",
```

to:

```python
trigger_dag_id="get_config",
```

- [ ] **Step 3: Delete old get_config DAGs**

```bash
rm dags/layer__get_config/src2brz__get_config.py
rm dags/layer__get_config/brz2sil__get_config.py
```

- [ ] **Step 4: Verify DAGs parse correctly**

```bash
docker compose exec airflow-scheduler airflow dags list 2>&1 | grep -E "get_config|coordinator"
```

Expected: `get_config` appears (not `src2brz_get_config` or `brz2sil_get_config`).

- [ ] **Step 5: Commit**

```bash
git add dags/layer__get_config/get_config.py dags/coordinator.py
git rm dags/layer__get_config/src2brz__get_config.py dags/layer__get_config/brz2sil__get_config.py
git commit -m "refactor: unify get_config DAGs into single parameterized DAG"
```

---

### Task 4: Collapse process_object DAGs into one parameterized DAG

**Files:**
- Create: `dags/layer__process_object/process_object.py`
- Delete: `dags/layer__process_object/src2brz__process_object.py`, `dags/layer__process_object/brz2sil__process_object.py`

Same pattern as Task 3 — the two process_object DAGs are nearly identical.

- [ ] **Step 1: Create the unified process_object DAG**

```python
# dags/layer__process_object/process_object.py
"""Unified process_object DAG — receives config for a single (data_subject, source) pair,
filters active tables, sorts by load_sequence, and triggers the execution DAG for the layer."""

import sys
from datetime import datetime

from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # type: ignore

sys.path.insert(0, "/opt/airflow")
from src.ingestion.audit import audited


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
```

**Important:** The `process_object` function must also return `execution_dag` so the template resolves correctly. Update the return dict:

```python
    execution_dag = EXECUTION_DAGS.get(layer)
    if not execution_dag:
        raise ValueError(f"No execution DAG mapped for layer '{layer}'")

    return {
        "button": conf.get("button"),
        "layer": layer,
        "data_subject": data_subject,
        "source": source,
        "tables": tables_sorted,
        "execution_dag": execution_dag,
    }
```

- [ ] **Step 2: Update get_config DAG trigger target**

In `dags/layer__get_config/get_config.py` (from Task 3), change the processing_trigger:

```python
    processing_trigger = TriggerDagRunOperator(
        task_id="processing_trigger",
        trigger_dag_id="process_object",
        conf="{{ ti.xcom_pull(task_ids='get_config') }}",
    )
```

Since process_object now reads the layer from conf and resolves the execution DAG dynamically, we no longer need per-layer trigger targets.

- [ ] **Step 3: Delete old process_object DAGs**

```bash
rm dags/layer__process_object/src2brz__process_object.py
rm dags/layer__process_object/brz2sil__process_object.py
```

- [ ] **Step 4: Verify DAGs parse correctly**

```bash
docker compose exec airflow-scheduler airflow dags list 2>&1 | grep -E "process_object"
```

Expected: Only `process_object` appears.

- [ ] **Step 5: Commit**

```bash
git add dags/layer__process_object/process_object.py dags/layer__get_config/get_config.py
git rm dags/layer__process_object/src2brz__process_object.py dags/layer__process_object/brz2sil__process_object.py
git commit -m "refactor: unify process_object DAGs into single parameterized DAG"
```

---

### Task 5: Replace hardcoded paths across all modules with settings

**Files:**
- Modify: `src/ingestion/bronze.py`
- Modify: `src/ingestion/stg.py`
- Modify: `src/ingestion/retention.py`
- Modify: `src/ingestion/alert.py`
- Modify: `src/ingestion/layer_management.py`
- Modify: `src/ingestion/audit/file_logger.py`
- Modify: `src/ingestion/audit/db_logger.py`
- Modify: `dags/layer__execution/src2brz__rdbms2parquet_ingestion.py`
- Modify: `dags/layer__execution/brz2stg__parquet2postgres_ingestion.py`
- Modify: `dags/layer__execution/stg2sil__process_whdata.py`
- Modify: `dags/monitoring/data_freshness_check.py`
- Modify: `dags/monitoring/bronze_file_retention.py`

Replace every hardcoded path with the corresponding import from `src.ingestion.settings`.

- [ ] **Step 1: Update src/ingestion/alert.py**

Replace:
```python
ALERT_CONFIG_PATH = Path("/opt/airflow/config/alert_config.csv")
```
With:
```python
from src.ingestion.settings import ALERT_CONFIG_PATH
```

- [ ] **Step 2: Update src/ingestion/retention.py**

Replace:
```python
BRONZE_DIR = Path("/opt/airflow/data/bronze")
```
With:
```python
from src.ingestion.settings import BRONZE_BASE_URL
BRONZE_DIR = Path(BRONZE_BASE_URL)
```

- [ ] **Step 3: Update src/ingestion/layer_management.py**

Replace:
```python
LAYER_MGMT_PATH = Path("/opt/airflow/config/layer_management_config.csv")
```
With:
```python
from src.ingestion.settings import LAYER_MANAGEMENT_CONFIG_PATH as LAYER_MGMT_PATH
```

- [ ] **Step 4: Update src/ingestion/audit/file_logger.py**

Replace:
```python
AUDIT_LOG_DIR = Path("/opt/airflow/logs/audit")
```
With:
```python
from src.ingestion.settings import AUDIT_LOG_DIR
```

- [ ] **Step 5: Update src/ingestion/audit/db_logger.py**

Replace:
```python
SECRETS_PATH = Path("/opt/airflow/.dlt/secrets.toml")
```
With:
```python
from src.ingestion.settings import SECRETS_PATH
```

- [ ] **Step 6: Update execution DAGs**

In `src2brz__rdbms2parquet_ingestion.py`, replace:
```python
SECRETS_PATH = Path("/opt/airflow/.dlt/secrets.toml")
BUCKET_URL = "/opt/airflow/data/bronze"
```
With:
```python
from src.ingestion.settings import SECRETS_PATH, BRONZE_BASE_URL as BUCKET_URL
```

In `brz2stg__parquet2postgres_ingestion.py`, replace:
```python
SECRETS_PATH = Path("/opt/airflow/.dlt/secrets.toml")
BRONZE_BASE_URL = "/opt/airflow/data/bronze"
```
With:
```python
from src.ingestion.settings import SECRETS_PATH, BRONZE_BASE_URL
```

In `stg2sil__process_whdata.py`, replace:
```python
SECRETS_PATH = Path("/opt/airflow/.dlt/secrets.toml")
DBT_PROJECT_DIR = Path("/opt/airflow/dbt")
```
With:
```python
from src.ingestion.settings import SECRETS_PATH, DBT_PROJECT_DIR
```

- [ ] **Step 7: Update monitoring DAGs**

In `data_freshness_check.py`, replace:
```python
SECRETS_PATH = Path("/opt/airflow/.dlt/secrets.toml")
FRESHNESS_CONFIG_PATH = Path("/opt/airflow/config/freshness_config.csv")
```
With:
```python
from src.ingestion.settings import SECRETS_PATH, FRESHNESS_CONFIG_PATH
```

In `bronze_file_retention.py`, replace:
```python
RETENTION_CONFIG_PATH = Path("/opt/airflow/config/retention_config.csv")
```
With:
```python
from src.ingestion.settings import RETENTION_CONFIG_PATH
```

- [ ] **Step 8: Run existing tests to verify no regressions**

```bash
python -m pytest tests/ -v
```

Expected: All existing tests pass.

- [ ] **Step 9: Commit**

```bash
git add src/ingestion/settings.py src/ingestion/alert.py src/ingestion/retention.py \
  src/ingestion/layer_management.py src/ingestion/audit/file_logger.py \
  src/ingestion/audit/db_logger.py dags/layer__execution/ dags/monitoring/
git commit -m "refactor: replace all hardcoded paths with centralized settings module"
```

---

### Task 6: Clean up dead code in bronze.py

**Files:**
- Modify: `src/ingestion/bronze.py`
- Modify: `tests/ingestion/test_bronze.py`

`bronze.py` has three overlapping orchestration functions that are no longer used by the DAGs:
- `run_source_ingestion()` — legacy, used only by `cli.py`
- `run_data_subject_ingestion()` — legacy, not called anywhere
- `run_all_sources()` — legacy, used only by `cli.py`
- `_group_tables_by_data_subject()` — helper for run_source_ingestion only
- `_print_bronze_summary()` — helper for run_source_ingestion only

The DAGs use `extract_tables()` + `load_to_parquet()` directly. The CLI (`cli.py`) uses `run_source_ingestion` but that can be updated to use the same functions.

- [ ] **Step 1: Remove dead functions from bronze.py**

Delete these functions from `src/ingestion/bronze.py`:
- `run_source_ingestion()` (lines 114-164)
- `run_data_subject_ingestion()` (lines 167-206)
- `_group_tables_by_data_subject()` (lines 92-98)
- `_print_bronze_summary()` (lines 101-111)
- `run_all_sources()` (lines 319-328)

- [ ] **Step 2: Update cli.py to use extract_tables + load_to_parquet**

In `src/ingestion/cli.py`, replace the call to `run_source_ingestion` with the extract + load pattern used by the DAGs. Update the `main()` function:

```python
def main():
    import argparse
    from src.ingestion.config import load_source_configs

    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config/src2brz_config.csv")
    parser.add_argument("--secrets", default=".dlt/secrets.toml")
    parser.add_argument("--bucket-url", default="data/bronze")
    parser.add_argument("--source", default=None)
    args = parser.parse_args()

    secrets = load_secrets(Path(args.secrets))
    sources = load_source_configs(Path(args.config))

    for source_config in sources:
        if args.source and source_config.name != args.source:
            continue
        credentials = secrets.get(source_config.name)
        if not credentials:
            print(f"[{source_config.name}] Skipping — no credentials")
            continue

        for data_subject in {t.data_subject for t in source_config.tables}:
            extract_tables(source_config, args.bucket_url, credentials, data_subject)
            load_to_parquet(source_config, args.bucket_url, data_subject)
```

- [ ] **Step 3: Run tests**

```bash
python -m pytest tests/ingestion/test_bronze.py -v
```

Expected: All tests pass (none of the removed functions were tested).

- [ ] **Step 4: Commit**

```bash
git add src/ingestion/bronze.py src/ingestion/cli.py
git commit -m "refactor: remove dead orchestration functions from bronze.py"
```

---

### Task 7: Make stg2sil dataset triggers config-driven

**Files:**
- Modify: `dags/layer__execution/stg2sil__process_whdata.py`
- Modify: `config/layer_management_config.csv`

Currently the stg2sil DAG has hardcoded dataset triggers:
```python
schedule=[
    Dataset("stg__accounting__postgres_crm"),
    Dataset("stg__accounting__postgres_timesheet"),
]
```

This means adding a new source requires editing Python code. Instead, derive the datasets from config.

- [ ] **Step 1: Add stg2sil trigger config to layer_management_config.csv**

Add rows for brz2sil → stg2sil auto-trigger:

```csv
source_layer,target_layer,data_subject,source,auto_trigger,active
src2brz,brz2sil,accounting,postgres_crm,1,1
src2brz,brz2sil,accounting,postgres_timesheet,1,1
src2brz,brz2sil,project,maria_erp,1,1
brz2sil,stg2sil,accounting,postgres_crm,1,1
brz2sil,stg2sil,accounting,postgres_timesheet,1,1
```

- [ ] **Step 2: Update stg2sil DAG to read datasets from config**

In `stg2sil__process_whdata.py`, replace hardcoded datasets:

```python
import csv
from src.ingestion.settings import LAYER_MANAGEMENT_CONFIG_PATH

def _get_stg_datasets():
    """Read layer_management_config.csv and return Dataset list for brz2sil → stg2sil triggers."""
    datasets = []
    if not LAYER_MANAGEMENT_CONFIG_PATH.exists():
        return datasets
    with open(LAYER_MANAGEMENT_CONFIG_PATH) as f:
        for row in csv.DictReader(f):
            if (
                row["source_layer"] == "brz2sil"
                and row["target_layer"] == "stg2sil"
                and row.get("active", "1").strip() in ("1", "TRUE", "true")
            ):
                datasets.append(Dataset(f"stg__{row['data_subject']}__{row['source']}"))
    return datasets

# At module level (DAG parse time):
_datasets = _get_stg_datasets()

with DAG(
    dag_id="stg2sil__process_whdata",
    schedule=_datasets if _datasets else None,
    ...
```

- [ ] **Step 3: Verify DAG parses correctly**

```bash
docker compose exec airflow-scheduler airflow dags list 2>&1 | grep stg2sil
```

Expected: `stg2sil__process_whdata` appears without errors.

- [ ] **Step 4: Commit**

```bash
git add dags/layer__execution/stg2sil__process_whdata.py config/layer_management_config.csv
git commit -m "refactor: make stg2sil dataset triggers config-driven"
```

---

### Task 8: Delete old config files and update dag_init_script.py

**Files:**
- Delete: `config/src2brz_config.csv`
- Delete: `config/brz2sil_config.csv`
- Modify: `dags/dag_init_script.py` (update CONFIG_PATH reference if needed)

- [ ] **Step 1: Delete redundant config files**

```bash
git rm config/src2brz_config.csv config/brz2sil_config.csv
```

- [ ] **Step 2: Update dag_init_script.py to use settings**

Replace:
```python
PROJECT_ROOT = Path(__file__).parent.parent
CONFIG_PATH = PROJECT_ROOT / "config" / "dag_config.csv"
DAGS_OUTPUT_DIR = PROJECT_ROOT / "dags" / "layer__data_subject__src"
```

With:
```python
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from src.ingestion.settings import DAG_CONFIG_PATH as CONFIG_PATH

DAGS_OUTPUT_DIR = Path(__file__).parent / "layer__data_subject__src"
```

- [ ] **Step 3: Run full test suite**

```bash
python -m pytest tests/ -v
```

Expected: All tests pass.

- [ ] **Step 4: Commit**

```bash
git add dags/dag_init_script.py
git rm config/src2brz_config.csv config/brz2sil_config.csv
git commit -m "refactor: remove redundant config files, update dag_init_script"
```

---

### Task 9: Fix stg.py test imports (broken test)

**Files:**
- Modify: `tests/ingestion/test_stg.py`

`test_stg.py` imports `get_recent_parquet_files` which does not exist in `stg.py`. These tests are currently broken.

- [ ] **Step 1: Remove broken test imports and tests**

Replace the import line:
```python
from src.ingestion.stg import (
    get_parquet_dir,
    get_recent_parquet_files,
    build_stg_pipeline,
)
```

With:
```python
from src.ingestion.stg import (
    get_parquet_dir,
    get_latest_parquet_file,
    build_stg_pipeline,
)
```

Remove the 4 tests that reference `get_recent_parquet_files`:
- `test_get_recent_parquet_files_returns_files_within_retention`
- `test_get_recent_parquet_files_includes_rotated_files`
- `test_get_recent_parquet_files_empty_dir`
- `test_get_recent_parquet_files_missing_dir`

Replace them with tests for `get_latest_parquet_file`:

```python
def test_get_latest_parquet_file_returns_most_recent(tmp_path):
    (tmp_path / "01-01-2026.parquet").write_bytes(b"old")
    (tmp_path / "15-03-2026.parquet").write_bytes(b"mid")
    (tmp_path / "25-03-2026.parquet").write_bytes(b"new")

    result = get_latest_parquet_file(str(tmp_path))
    assert result is not None
    assert result.name == "25-03-2026.parquet"


def test_get_latest_parquet_file_empty_dir(tmp_path):
    result = get_latest_parquet_file(str(tmp_path))
    assert result is None


def test_get_latest_parquet_file_missing_dir():
    result = get_latest_parquet_file("/nonexistent/path")
    assert result is None
```

- [ ] **Step 2: Update build_stg_pipeline test**

The `build_stg_pipeline` signature changed — it now takes `source_name` and `data_subject` instead of `source_config`. Update the test:

```python
def test_build_stg_pipeline():
    pipeline = build_stg_pipeline(
        source_name="postgres_crm",
        data_subject="accounting",
        warehouse_credentials="postgresql://user:pass@localhost:5432/test_db",
    )
    assert pipeline.pipeline_name == "stg_postgres_crm_accounting"
    assert pipeline.destination.destination_name == "postgres"
    assert pipeline.dataset_name == "stg__accounting__postgres_crm"
```

- [ ] **Step 3: Run tests**

```bash
python -m pytest tests/ingestion/test_stg.py -v
```

Expected: All tests pass.

- [ ] **Step 4: Commit**

```bash
git add tests/ingestion/test_stg.py
git commit -m "fix: update stg tests to match current module interface"
```

---

### Task 10: Final cleanup and validation

- [ ] **Step 1: Run full test suite**

```bash
python -m pytest tests/ -v
```

Expected: All tests pass.

- [ ] **Step 2: Verify all DAGs parse in Airflow**

```bash
docker compose exec airflow-scheduler airflow dags list
```

Expected:
- `coordinator` — present
- `get_config` — present (unified)
- `process_object` — present (unified)
- `src2brz_rdbms2parquet_ingestion` — present
- `brz2stg_parquet2postgres_ingestion` — present
- `stg2sil__process_whdata` — present
- `bronze_file_retention` — present
- `data_freshness_check` — present
- `dag_generator` — present
- 6 button DAGs — present

NOT present (deleted):
- `src2brz_get_config`
- `brz2sil_get_config`
- `src2brz__process_object`
- `brz2sil__process_object`

- [ ] **Step 3: Trigger an end-to-end test run**

Trigger `src2brz__accounting__postgres_crm` button and verify the full chain:
1. Button → Coordinator
2. Coordinator → get_config (unified)
3. get_config → process_object (unified)
4. process_object → src2brz_rdbms2parquet_ingestion
5. trigger_next_layer → brz2sil button → Coordinator (repeat for brz2stg)

- [ ] **Step 4: Final commit**

```bash
git add -A
git commit -m "refactor: complete code quality, architecture, and config unification"
```
