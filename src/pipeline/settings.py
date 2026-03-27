"""Centralized paths and constants — single source of truth for the entire project."""

import os
from pathlib import Path

AIRFLOW_HOME = Path(os.environ.get("AIRFLOW_HOME", "/opt/airflow"))

SECRETS_PATH = AIRFLOW_HOME / ".dlt" / "secrets.toml"
BRONZE_BASE_PATH = AIRFLOW_HOME / "data" / "bronze"
BRONZE_BASE_URL = str(BRONZE_BASE_PATH)
CONFIG_DIR = AIRFLOW_HOME / "config"
DBT_PROJECT_DIR = AIRFLOW_HOME / "dbt"
AUDIT_LOG_DIR = AIRFLOW_HOME / "logs" / "audit"

TABLE_CONFIG_PATH = CONFIG_DIR / "table_config.csv"
LAYER_MANAGEMENT_CONFIG_PATH = CONFIG_DIR / "layer_management_config.csv"
ALERT_CONFIG_PATH = CONFIG_DIR / "alert_config.csv"
FRESHNESS_CONFIG_PATH = CONFIG_DIR / "freshness_config.csv"
RETENTION_CONFIG_PATH = CONFIG_DIR / "retention_config.csv"
DAG_CONFIG_PATH = CONFIG_DIR / "dag_config.csv"
