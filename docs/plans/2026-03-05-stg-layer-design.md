# Staging Layer Design

## Overview

Load bronze parquet files into PostgreSQL staging tables using dlt, then create deduplicated "newest" tables using dbt. The Stg layer sits between Bronze (parquet files) and Silver (SCD-2 / dim tables).

## Data Flow

```
Bronze parquet files (local filesystem)
    │
    ├─ dlt (filesystem readers → postgres destination)
    │
    ▼
stg_temp schema (Postgres)          ← Raw daily loads, append-only, 7-day retention
    │  ├── project_task
    │  └── account_account
    │
    ├─ dbt models (DISTINCT ON primary key)
    │
    ▼
stg schema (Postgres)               ← Newest record per PK, full refresh each run
    ├── project_task
    └── account_account
```

## Key Decisions

- **dlt for data movement**, dbt for transformations — clear separation of concerns
- **stg_temp** is append-only with 7-day rolling retention (bronze parquet is the permanent archive)
- **stg (newest)** is materialized as `table` (full refresh) by dbt, deduplicated by primary key
- **Primary keys** defined in `sources.yaml`, supporting single or composite keys
- **External Postgres** as the warehouse (not the Airflow metadata DB)

## Postgres Schemas

| Schema | Purpose | Write strategy | Retention |
|--------|---------|---------------|-----------|
| `stg_temp` | Raw parquet loads | Append | 7 days |
| `stg` | Newest record per PK | Full refresh (dbt) | Current state only |

## Config Changes

### sources.yaml — add `primary_key` field

```yaml
sources:
  - name: postgres_crm
    data_subject: crm
    schema: public
    tables:
      - name: project_task
        load_strategy: full
        primary_key: id                    # single column

  - name: postgres_timesheet
    data_subject: hr
    schema: public
    tables:
      - name: account_account
        load_strategy: incremental
        cursor_column: write_date
        initial_value: "2024-01-01"
        primary_key: id                    # single column
```

Composite keys are supported as a list:

```yaml
primary_key: [org_id, user_id]
```

### secrets.toml — add warehouse credentials

```toml
[destinations.warehouse]
credentials = "postgresql://user:password@host:5432/warehouse_db"
```

## dlt: Parquet → stg_temp

Uses dlt's `filesystem` source with `readers().read_parquet()` to read bronze parquet files and load into Postgres.

- **Source**: `readers(bucket_url, file_glob="**/*.parquet").read_parquet()`
- **Destination**: `postgres` with `dataset_name="stg_temp"`
- **Write disposition**: `append`
- **File tracking**: dlt's built-in filesystem state tracks which files have been processed (no re-loading)
- dlt automatically adds `_dlt_load_id` to each row for ordering

## dbt: stg_temp → stg (newest)

### Project Structure

```
dbt/
├── dbt_project.yml
├── profiles.yml
├── models/
│   └── stg/
│       ├── _stg_sources.yml         # dbt source definitions (stg_temp schema)
│       ├── stg_project_task.sql
│       └── stg_account_account.sql
```

### Model Pattern

Each stg model follows the same deduplication pattern:

```sql
-- stg_project_task.sql
{{ config(materialized='table', schema='stg') }}

SELECT DISTINCT ON (id)
    *
FROM {{ source('stg_temp', 'project_task') }}
ORDER BY id, _dlt_load_id DESC
```

For composite keys:

```sql
SELECT DISTINCT ON (org_id, user_id)
    *
FROM {{ source('stg_temp', 'some_table') }}
ORDER BY org_id, user_id, _dlt_load_id DESC
```

### Source Definition

```yaml
# _stg_sources.yml
version: 2
sources:
  - name: stg_temp
    schema: stg_temp
    tables:
      - name: project_task
      - name: account_account
```

## Cleanup: 7-Day Retention

After each run, delete old rows from `stg_temp`:

```sql
DELETE FROM stg_temp.{table}
WHERE _dlt_load_id < (
    SELECT _dlt_load_id FROM stg_temp._dlt_loads
    WHERE inserted_at < NOW() - INTERVAL '7 days'
)
```

Alternatively, a simpler approach using a `_loaded_at` timestamp added during dlt load.

## Airflow Integration

New DAG `stg_ingestion` runs after `bronze_ingestion`:

```
bronze_ingestion → stg_ingestion
```

Steps per source in `stg_ingestion`:
1. **dlt load** — read parquet from `data/bronze/` → `stg_temp` tables in Postgres
2. **dbt run** — build newest tables in `stg` schema
3. **cleanup** — delete `stg_temp` rows older than 7 days

## How This Supports Silver Layer

The `stg` (newest) tables provide a clean "current state" for each record. dbt snapshots in the Silver layer will:
- Compare `stg.{table}` against previous snapshot state
- Generate SCD-2 records (valid_from, valid_to, is_current)
- Overwrite dimension tables directly from `stg`
