# Bronze Ingestion Layer Design

## Overview

Daily batch ingestion from three source databases into a bronze layer of parquet files on the local filesystem, using dlt (data load tool).

## Sources

| Source | Database Type | Data Subject |
|--------|--------------|--------------|
| PostgreSQL CRM | PostgreSQL | crm |
| PostgreSQL Timesheet | PostgreSQL | hr |
| MariaDB ERP | MariaDB | erp |

## Approach

**dlt `sql_database` source** with `filesystem` destination writing parquet files via the `pyarrow` backend.

- One dlt pipeline per source database (isolates state, failures don't cascade)
- Config-driven: tables and load strategies defined in `config/sources.yaml`
- Credentials stored in `.dlt/secrets.toml` (gitignored)

## File Output Structure

```
data/bronze/{data_subject}/{src_db}/{src_schema}/{table_name}/{DD-MM-YYYY}.parquet
```

Example: `data/bronze/crm/postgres_crm/public/customers/03-03-2026.parquet`

## Project Layout

```
etl-pipeline-file-partition-demo/
├── config/
│   └── sources.yaml          # Table + source definitions
├── src/
│   └── ingestion/
│       ├── __init__.py
│       └── bronze.py          # Main ingestion logic
├── .dlt/
│   ├── config.toml            # dlt non-sensitive config
│   └── secrets.toml           # Connection strings (gitignored)
├── data/
│   └── bronze/                # Parquet output (gitignored)
├── requirements.txt
├── .gitignore
└── README.md
```

## Config Format

### `config/sources.yaml`

```yaml
sources:
  - name: postgres_crm
    data_subject: crm
    schema: public
    tables:
      - name: customers
        load_strategy: full
      - name: orders
        load_strategy: incremental
        cursor_column: updated_at
        initial_value: "2024-01-01"

  - name: postgres_timesheet
    data_subject: hr
    schema: public
    tables:
      - name: timesheets
        load_strategy: incremental
        cursor_column: updated_at
        initial_value: "2024-01-01"

  - name: mariadb_erp
    data_subject: erp
    schema: erp_db
    tables:
      - name: invoices
        load_strategy: incremental
        cursor_column: modified_at
        initial_value: "2024-01-01"
```

### `.dlt/secrets.toml`

```toml
[sources.postgres_crm]
credentials = "postgresql://user:pass@host:5432/crm_db"

[sources.postgres_timesheet]
credentials = "postgresql://user:pass@host:5432/timesheet_db"

[sources.mariadb_erp]
credentials = "mysql+pymysql://user:pass@host:3306/erp_db"
```

## How It Works

1. Python script reads `config/sources.yaml`
2. For each source, creates a dlt pipeline with `filesystem` destination
3. Uses `extra_placeholders` to inject `data_subject`, `src_db`, `src_schema` into the layout path
4. Per table: applies `dlt.sources.incremental` hints if configured, otherwise does full extract
5. dlt writes parquet files via the `pyarrow` backend
6. Layout template: `{data_subject}/{src_db}/{src_schema}/{table_name}/{DD}-{MM}-{YYYY}.{ext}`

## Key Decisions

- **pyarrow backend** — efficient SQL -> Arrow -> Parquet path, no intermediate serialization
- **One pipeline per source** — isolated state and failure domains
- **YAML config** — add/remove tables without changing Python code
- **dlt state tracking** — incremental cursor values persisted automatically between runs
- **Local filesystem first** — simple for demo, can swap to S3/MinIO later by changing `bucket_url`
