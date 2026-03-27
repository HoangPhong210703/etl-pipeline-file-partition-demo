# ETL Pipeline — Architecture & Codebase Guide

## Table of Contents

1. [Overview](#overview)
2. [Three-Layer Architecture](#three-layer-architecture)
3. [Repository Structure](#repository-structure)
4. [Data Flow Walkthrough](#data-flow-walkthrough)
5. [DAG Catalog](#dag-catalog)
6. [src/pipeline Module Reference](#srcpipeline-module-reference)
7. [Configuration Files](#configuration-files)
8. [Infrastructure](#infrastructure)
9. [Development Guide](#development-guide)

---

## Overview

A metadata-driven, config-first ETL pipeline built on **Apache Airflow 2.10.4**, **dlt** (data load tool), and **dbt**. It ingests data from multiple RDBMS sources, persists it as Parquet files (bronze layer), loads it into a PostgreSQL warehouse (staging layer), and transforms it into analytics-ready silver models via dbt.

**Current data sources and subjects:**

| Source | Type | Data Subject |
|--------|------|-------------|
| `postgres_crm` | PostgreSQL | accounting |
| `postgres_timesheet` | PostgreSQL | accounting |
| `maria_erp` | MariaDB | project |

51 tables total, all defined in `config/table_config.csv`.

---

## Three-Layer Architecture

```
Source DBs          Bronze Layer          Staging Layer         Silver Layer
(RDBMS)         (Parquet on disk)     (PostgreSQL schemas)  (dbt models)

postgres_crm ──►  data/bronze/        stg__accounting__     silver__accounting__
postgres_ts  ──►  accounting/         postgres_crm          account_account
maria_erp    ──►  project/            stg__accounting__
                                      postgres_timesheet
                                      stg__project__
                                      maria_erp
```

| Layer | Trigger Name | Technology | What it does |
|-------|-------------|------------|--------------|
| `src2brz` | src → bronze | dlt + filesystem | Extract tables from RDBMS, write Parquet per table per day |
| `brz2sil` | bronze → staging | dlt + postgres | Read latest Parquet, load into `stg__*` schemas in warehouse |
| `stg2sil` | staging → silver | dbt | Run stg models → snapshots → silver models → tests |

---

## Repository Structure

```
etl-pipeline-file-partition-demo/
│
├── config/                          # All runtime configuration (CSV)
│   ├── table_config.csv             # Table definitions for all layers
│   ├── dag_config.csv               # Button DAG generation spec
│   ├── layer_management_config.csv  # Auto-trigger rules between layers
│   ├── freshness_config.csv         # Staleness thresholds per source
│   ├── retention_config.csv         # Parquet file retention windows
│   └── alert_config.csv             # Alert recipients per event type
│
├── dags/                            # Airflow DAG definitions
│   ├── coordinator.py               # Entry point — parses button DAG IDs
│   ├── dag_init_script.py           # Script: generates button DAGs from dag_config.csv
│   ├── layer__data_subject__src/    # Auto-generated button DAGs (one per source/subject)
│   ├── layer__get_config/           # get_config.py — reads table config
│   ├── layer__process_object/       # process_object.py — filters/sorts tables
│   ├── layer__execution/            # Three execution DAGs (one per layer)
│   └── monitoring/                  # data_freshness_check.py, bronze_file_retention.py
│
├── src/pipeline/                    # All business logic (no Airflow dependencies)
│   ├── settings.py                  # Centralized paths & constants
│   ├── config.py                    # CSV config parsing + dataclasses
│   ├── credentials.py               # Credential loading from .dlt/secrets.toml
│   ├── bronze.py                    # src2brz extraction logic (dlt)
│   ├── staging.py                   # brz2stg load logic (dlt + postgres)
│   ├── dbt_runner.py                # dbt subprocess wrappers
│   ├── layer_management.py          # Layer trigger resolution
│   ├── freshness.py                 # Freshness check logic
│   ├── retention.py                 # Parquet file cleanup logic
│   ├── alert.py                     # Email alerting
│   ├── dag_generator.py             # Button DAG file generation
│   ├── audit/                       # Audit logging (file + DB)
│   │   ├── __init__.py              # log_audit() + @audited decorator export
│   │   ├── decorator.py             # @audited wraps tasks with timing + logging
│   │   ├── file_logger.py           # Writes .log files to logs/audit/
│   │   └── db_logger.py             # Writes to meta.* tables in warehouse
│   └── cli/                         # Command-line entry points
│       ├── bronze_cli.py            # Run bronze layer from terminal
│       └── staging_cli.py           # Run staging layer from terminal
│
├── dbt/                             # dbt project
│   ├── dbt_project.yml
│   ├── profiles.yml                 # Connects via WAREHOUSE_* env vars
│   ├── macros/                      # silver_dedup, generate_schema_name
│   ├── models/silver/               # Silver layer SQL models
│   └── snapshots/                   # SCD-2 snapshot definitions
│
├── data/bronze/                     # Parquet storage (volume-mounted)
│   └── {data_subject}/{source}/{schema}/{table}/{DD}-{MM}-{YYYY}.parquet
│
├── logs/audit/                      # Plain-text audit logs (volume-mounted)
│   └── {dag_id}/{YYYY-MM-DD}.log
│
├── tests/pipeline/                  # pytest test suite
├── .dlt/                            # dlt secrets + config (volume-mounted)
│   └── secrets.toml                 # Source and warehouse credentials
├── docker-compose.yaml
├── Dockerfile
├── requirements.txt
└── CodingConvention.md
```

---

## Data Flow Walkthrough

### Manual trigger path (src2brz example)

```
User clicks "Trigger DAG" on:
  src2brz__accounting__postgres_crm
          │
          ▼
[coordinator]
  coor task
    Parses button = "src2brz__accounting__postgres_crm"
    Extracts: layer=src2brz, data_subject=accounting, source=postgres_crm
    XCom returns: {button, layer, data_subject, source}
          │
          ▼ TriggerDagRunOperator
[get_config]
  get_config task
    Reads config/table_config.csv
    Filters: data_subject=accounting AND source=postgres_crm
    Returns: {button, layer, data_subject, source, tables: [...22 rows...]}
          │
          ▼ TriggerDagRunOperator
[process_object]
  process_object task
    Filters: table_load_active=1
    Sorts: by load_sequence (10, 20, 40, 60)
    Resolves: execution_dag = "src2brz_rdbms2parquet_ingestion"
          │
          ▼ TriggerDagRunOperator
[src2brz_rdbms2parquet_ingestion]
  rdbms_src_connect  → test DB connection
  fetch_tables       → dlt extract (RDBMS → normalized state, 3-attempt retry per table)
  write_parquet      → dlt load (normalized state → Parquet files on disk)
  trigger_next_layer → check layer_management_config.csv
                        auto_trigger=1 → trigger brz2sil__accounting__postgres_crm
```

### Auto-trigger chain (brz2stg → stg2sil)

```
[brz2stg_parquet2postgres_ingestion]
  verify_parquet     → check Parquet files exist
  load_to_warehouse  → dlt load (Parquet → stg__accounting__postgres_crm in warehouse)
                        emits Dataset("stg__accounting__postgres_crm")
                               │
                               ▼ Dataset trigger (when ALL subscribed datasets arrive)
[stg2sil__process_whdata]
  write_dag_run_note → record which datasets triggered this run
  run_dbt_stg        → dbt run --select stg
  run_dbt_snapshot   → dbt snapshot (SCD-2)
  run_dbt_silver     → dbt run --select silver
  test_dbt           → dbt test + log results to meta.dbt_test_results
```

### Dataset-based stg2sil scheduling

`stg2sil__process_whdata` subscribes to all datasets defined in `layer_management_config.csv` where `source_layer=brz2sil` and `target_layer=stg2sil`. The DAG schedule is built at parse time:

```python
_stg_dicts = get_stg_datasets()   # reads layer_management_config.csv
_datasets = [Dataset(f"stg__{d['data_subject']}__{d['source']}") for d in _stg_dicts]
```

The DAG runs only when **all** subscribed datasets have fired in the same logical date window.

---

## DAG Catalog

### Orchestration DAGs

| DAG ID | Schedule | Triggered by | Purpose |
|--------|----------|-------------|---------|
| `coordinator` | None | Button DAGs | Parse button ID → route to get_config |
| `get_config` | None | coordinator | Read table_config.csv for (data_subject, source) |
| `process_object` | None | get_config | Filter active tables, sort, pick execution DAG |

### Execution DAGs

| DAG ID | Schedule | Layer | Purpose |
|--------|----------|-------|---------|
| `src2brz_rdbms2parquet_ingestion` | None | src2brz | RDBMS → Parquet |
| `brz2stg_parquet2postgres_ingestion` | None | brz2sil | Parquet → warehouse (stg schema) |
| `stg2sil__process_whdata` | Dataset | stg2sil | dbt stg → snapshot → silver → test |

### Button DAGs (auto-generated)

One DAG per (layer, data_subject, source) combination, generated from `dag_config.csv` by running `python dags/dag_init_script.py`.

| DAG ID | Triggers |
|--------|---------|
| `src2brz__accounting__postgres_crm` | coordinator → ... → src2brz ingestion |
| `src2brz__accounting__postgres_timesheet` | coordinator → ... → src2brz ingestion |
| `src2brz__project__maria_erp` | coordinator → ... → src2brz ingestion |
| `brz2sil__accounting__postgres_crm` | coordinator → ... → brz2stg ingestion |
| `brz2sil__accounting__postgres_timesheet` | coordinator → ... → brz2stg ingestion |
| `brz2sil__project__maria_erp` | coordinator → ... → brz2stg ingestion |

### Monitoring DAGs

| DAG ID | Schedule | Purpose |
|--------|----------|---------|
| `data_freshness_check` | `0 8 * * *` | Query meta.pipeline_audit; alert on sources stale > threshold |
| `bronze_file_retention` | `0 2 * * *` | Delete Parquet files older than retention_days |

---

## src/pipeline Module Reference

### settings.py
Single source of truth for all filesystem paths. All other modules import from here — never hardcode paths.

```python
AIRFLOW_HOME          # /opt/airflow (or AIRFLOW_HOME env var)
SECRETS_PATH          # AIRFLOW_HOME/.dlt/secrets.toml
BRONZE_BASE_PATH      # AIRFLOW_HOME/data/bronze
BRONZE_BASE_URL       # str(BRONZE_BASE_PATH)
CONFIG_DIR            # AIRFLOW_HOME/config
DBT_PROJECT_DIR       # AIRFLOW_HOME/dbt
AUDIT_LOG_DIR         # AIRFLOW_HOME/logs/audit
TABLE_CONFIG_PATH     # CONFIG_DIR/table_config.csv
LAYER_MANAGEMENT_CONFIG_PATH
ALERT_CONFIG_PATH
FRESHNESS_CONFIG_PATH
RETENTION_CONFIG_PATH
DAG_CONFIG_PATH
```

---

### config.py
Parses `table_config.csv` into typed dataclasses and provides filtering utilities.

**Dataclasses:**
- `CsvTableConfig` — direct mapping of one CSV row (all 11 fields)
- `SourceConfig` — grouped view: `{name, schema, tables: [TableConfig]}`
- `TableConfig` — minimal table metadata used by the staging layer

**Key functions:**

| Function | Returns | Description |
|----------|---------|-------------|
| `load_csv_config(path)` | `list[CsvTableConfig]` | Parse entire CSV |
| `get_active_tables(configs)` | `list[CsvTableConfig]` | Filter active, sort by load_sequence |
| `get_data_subjects(configs)` | `list[str]` | Unique data_subject values |
| `csv_to_source_configs(configs)` | `list[SourceConfig]` | Group by source_name |
| `load_source_configs(path)` | `list[SourceConfig]` | Load CSV + return active as SourceConfig |
| `csv_table_config_to_dict(c)` | `dict` | Serialize for XCom / TriggerDagRunOperator |
| `csv_table_config_from_dict(d)` | `CsvTableConfig` | Deserialize from XCom conf |
| `table_config_from_dict(d)` | `TableConfig` | Build staging TableConfig from conf dict |

---

### credentials.py
Loads credentials from `.dlt/secrets.toml`. Never inline TOML parsing elsewhere.

```toml
# .dlt/secrets.toml structure
[sources.postgres_crm]
credentials = "postgresql://user:pass@host:5432/db"

[sources.maria_erp]
credentials = "mysql://user:pass@host:3306/db"

[destinations.warehouse]
credentials = "postgresql://user:pass@host:5432/warehouse"
```

| Function | Description |
|----------|-------------|
| `load_source_credentials(source_name)` | Get connection string for a named source |
| `load_warehouse_credentials()` | Get warehouse connection string |
| `load_all_source_credentials()` | `{source_name: conn_str}` dict for all sources |

---

### bronze.py
Wraps dlt to extract RDBMS tables and write Parquet files.

**Parquet file layout:**
```
data/bronze/{data_subject}/{source_name}/{source_schema}/{table_name}/{DD}-{MM}-{YYYY}.parquet
```

If a run happens twice in the same day, `rotate_todays_parquet()` renames the existing file with a numeric suffix before writing a new one.

**Key functions:**

| Function | Description |
|----------|-------------|
| `test_source_connection(credentials, schema)` | Connectivity check before extraction |
| `extract_tables(source_config, bucket_url, credentials, data_subject)` | Extract all tables with 3-attempt per-table retry. Raises if any table fails all retries |
| `load_to_parquet(source_config, bucket_url, data_subject)` | Write normalized dlt state to Parquet |
| `rotate_todays_parquet(bucket_url, data_subject, source, schema, table)` | Rename existing today's file with `_1`, `_2`... suffix |

---

### staging.py
Loads Parquet files into the PostgreSQL warehouse using dlt.

**Key functions:**

| Function | Description |
|----------|-------------|
| `get_parquet_dir(bronze_base_url, data_subject, source_name, schema, table_name)` | Build path to a table's Parquet directory |
| `get_latest_parquet_file(parquet_dir)` | Return most recent `.parquet` file by mtime |
| `build_stg_pipeline(source_name, data_subject, credentials)` | Create dlt pipeline pointing to warehouse |
| `run_stg_subject(source_name, schema, data_subject, tables, bronze_base_url, credentials)` | Load all tables for one (data_subject, source) pair. Returns `list[(table_name, status, ...)]` |
| `run_stg_ingestion(source_config, bronze_base_url, credentials)` | Full ingestion for a SourceConfig (multiple data_subjects) |

dlt schema for staging: `stg__{data_subject}__{source_name}` (e.g. `stg__accounting__postgres_crm`).

---

### dbt_runner.py
Subprocess wrappers for dbt commands. No Airflow dependencies.

| Function | Description |
|----------|-------------|
| `set_dbt_env_vars(credentials)` | Parse connection string → set `WAREHOUSE_HOST/PORT/USER/PASSWORD/DB` env vars |
| `run_dbt(dbt_dir, selectors)` | `dbt run --select {selectors}` |
| `run_dbt_snapshot(dbt_dir)` | `dbt snapshot` |
| `run_dbt_test(dbt_dir)` | `dbt test` — logs warnings, does not raise on failure |
| `parse_dbt_results(dbt_dir)` | Parse `target/run_results.json` → `list[{test_name, status, failures, execution_time, message}]` |

---

### freshness.py
All freshness-check logic. Called by `data_freshness_check` DAG.

| Function | Description |
|----------|-------------|
| `load_freshness_thresholds()` | Read `freshness_config.csv`, return active threshold dicts |
| `check_freshness()` | Query `meta.pipeline_audit` for last successful src2brz per source, evaluate staleness, log to DB + file, send alerts for any stale sources |

---

### layer_management.py
Reads `layer_management_config.csv` to control auto-triggering.

| Function | Description |
|----------|-------------|
| `get_next_layer(current_layer, data_subject, source)` | Return target button dag_id if `auto_trigger=1`, else `None` |
| `get_stg_datasets()` | Return `[{data_subject, source}]` for all active `brz2sil → stg2sil` rows — used to build stg2sil Dataset schedule |

---

### audit/

#### @audited decorator (`audit/decorator.py`)
Apply to any Airflow PythonOperator callable. Automatically:
- Extracts `dag_id`, `task_id`, `run_id` from Airflow context
- Extracts `layer`, `source`, `data_subject` from `dag_run.conf`
- Sets a DagRun note visible in Airflow Grid view (`Button: {dag_id}`)
- Captures `row_count` from the task's return value (if dict with `row_count` key)
- Logs `started_at`, `finished_at`, duration, status to both file and DB
- Re-raises exceptions — never swallows failures

```python
@audited
def my_task(**kwargs):
    ...
    return {"row_count": 1234}   # row_count captured automatically
```

#### audit/db_logger.py
Writes to three tables in the `meta` schema (auto-created on first write):
- `meta.pipeline_audit` — one row per task execution
- `meta.dbt_test_results` — one row per dbt test result
- `meta.freshness_check_results` — one row per source per freshness check

#### audit/file_logger.py
Writes plain-text log files:
```
logs/audit/{dag_id}/{YYYY-MM-DD}.log
```
Format: `[YYYY-MM-DD HH:MM:SS] STATUS task_id | source=... | subject=... | rows=N (12.3s)`

---

### alert.py
Email notifications via `yagmail`. Requires `SENDER_EMAIL` and `SENDER_PASSWORD` env vars (Gmail App Password recommended). Gracefully skips if credentials are absent.

Recipients per alert type are configured in `config/alert_config.csv`.

Alert types: `pipeline_failure`, `pipeline_success`, `data_freshness`.

`yagmail` and `keyring` are imported lazily inside `send_alert()` — they are not loaded at DAG parse time.

---

### dag_generator.py
Generates button DAG files from `dag_config.csv`. Each generated file contains a single `TriggerDagRunOperator` pointing at the coordinator with `conf={"button": DAG_ID}`.

To regenerate after modifying `dag_config.csv`:
```bash
python dags/dag_init_script.py
```

---

## Configuration Files

### table_config.csv
Defines all tables ingested by the pipeline.

| Column | Description |
|--------|-------------|
| `id` | Unique row identifier |
| `table_name` | Table name in source DB and target stg schema |
| `source_name` | Source key matching `.dlt/secrets.toml` (`postgres_crm`, `maria_erp`, etc.) |
| `source_schema` | Schema in source DB (`public`, `appdb`) |
| `data_subject` | Business domain grouping (`accounting`, `project`) |
| `load_strategy` | `full`, `incremental`, or `append` |
| `cursor_column` | Column used for incremental load watermark (e.g. `write_date`) |
| `initial_value` | Initial cursor value for first incremental run |
| `primary_key` | Primary key column (used for dlt deduplication) |
| `load_sequence` | Execution order within a source (10, 20, 40, 60) |
| `table_load_active` | `1` = active, `0` = skip |

### layer_management_config.csv
Controls auto-triggering between layers.

| Column | Description |
|--------|-------------|
| `source_layer` | Layer that just completed (`src2brz`, `brz2sil`) |
| `target_layer` | Layer to trigger next (`brz2sil`, `stg2sil`) |
| `data_subject` | Scope of this rule |
| `source` | Source name scope |
| `auto_trigger` | `1` = automatically trigger next layer on success |
| `active` | `1` = rule is active |

### dag_config.csv
Template data for auto-generating button DAGs.

| Column | Description |
|--------|-------------|
| `layer__data_subject__src` | DAG ID (also the button label) |
| `schedule_interval` | Cron expression or `None` |
| `trigger_target` | Always `coordinator` |
| `tags` | Comma-separated list |
| `dag_active` | `1` = include in generation |

### freshness_config.csv
Staleness thresholds. The `data_freshness_check` DAG alerts if `MAX(finished_at)` for successful `src2brz` runs exceeds the threshold.

| Column | Description |
|--------|-------------|
| `source_name` | Source to monitor |
| `data_subject` | Data subject scope |
| `max_stale_hours` | Alert threshold in hours |
| `active` | `1` = enabled |

### retention_config.csv
Retention window for Parquet files in the bronze layer.

| Column | Description |
|--------|-------------|
| `source_name` | Source whose files to clean |
| `data_subject` | Data subject scope |
| `source_schema` | Schema subfolder in bronze path |
| `retention_days` | Delete files older than this many days |
| `active` | `1` = enabled |

### alert_config.csv
Recipients for each alert type.

| Column | Description |
|--------|-------------|
| `alert_type` | `pipeline_failure`, `pipeline_success`, or `data_freshness` |
| `recipients` | Semicolon-separated email list |
| `active` | `1` = enabled |

---

## Infrastructure

### Docker Compose Services

| Service | Image | Port | Purpose |
|---------|-------|------|---------|
| `airflow-db` | postgres:16 | — | Airflow metadata database |
| `airflow-init` | Dockerfile | — | Run once: `db migrate` + create admin user |
| `airflow-webserver` | Dockerfile | 8080 | Airflow UI |
| `airflow-scheduler` | Dockerfile | — | DAG parsing + task scheduling |

### Volume Mounts

| Host Path | Container Path | Purpose |
|-----------|---------------|---------|
| `./dags` | `/opt/airflow/dags` | DAG files |
| `./src` | `/opt/airflow/src` | Pipeline source code |
| `./config` | `/opt/airflow/config` | CSV config files |
| `./.dlt` | `/opt/airflow/.dlt` | Secrets and dlt config |
| `./data` | `/opt/airflow/data` | Parquet bronze layer storage |
| `./dbt` | `/opt/airflow/dbt` | dbt project |
| `./logs` | `/opt/airflow/logs` | Airflow + audit logs |

### Starting the stack

```bash
# First time
docker compose up --build -d

# Subsequent runs
docker compose up -d

# View logs
docker compose logs -f airflow-scheduler

# Regenerate button DAGs after editing dag_config.csv
python dags/dag_init_script.py
```

Airflow UI: http://localhost:8080 (admin / admin)

---

## Development Guide

### Adding a new source table

1. Add a row to `config/table_config.csv` with the new table details.
2. Add credentials to `.dlt/secrets.toml` if it's a new source.
3. If it's a new source entirely, add a row to `config/dag_config.csv` for each layer button and re-run `python dags/dag_init_script.py`.
4. Add a `layer_management_config.csv` row if auto-triggering is desired.

### Adding a new silver model

1. Create `dbt/models/silver/silver__{domain}__{model_name}.sql`.
2. Add model definition and tests to `dbt/models/silver/_silver_models.yml`.
3. The `stg2sil__process_whdata` DAG will pick it up automatically on next run.

### Running tests

```bash
# Activate venv first
source .venv/Scripts/activate   # Windows Git Bash

python -m pytest tests/ -v
```

### Running the pipeline without Docker (CLI)

```bash
# Bronze layer
python -m src.pipeline.cli.bronze_cli --source postgres_crm

# Staging layer
python -m src.pipeline.cli.staging_cli
```

### Audit log locations

| What | Where |
|------|-------|
| Per-task plain-text logs | `logs/audit/{dag_id}/{YYYY-MM-DD}.log` |
| Pipeline audit (DB) | `meta.pipeline_audit` in warehouse |
| dbt test results (DB) | `meta.dbt_test_results` in warehouse |
| Freshness check results (DB) | `meta.freshness_check_results` in warehouse |

### Key design rules

- **DAGs contain no business logic** — all logic lives in `src/pipeline/`. DAG files only wire together tasks and pass context.
- **No top-level heavy imports in DAGs** — all `src.pipeline.*` imports happen inside task function bodies, not at module level. Only `@audited`, `dag_failure_callback`, and `dag_success_callback` are top-level (required for decoration and DAG definition).
- **No inline credential loading** — always use `src.pipeline.credentials`. Never parse `.dlt/secrets.toml` directly in a DAG or task.
- **No hardcoded paths** — always import from `src.pipeline.settings`.
- **@audited on every task callable** — provides automatic timing, error capture, and row count logging.
- **Config changes take effect immediately** — editing any CSV in `config/` changes pipeline behavior on the next DAG run without code changes or restarts.
