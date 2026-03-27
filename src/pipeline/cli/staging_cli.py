"""CLI entry point for staging layer ingestion (Parquet → Postgres + dbt)."""

import argparse
from pathlib import Path

from src.pipeline.config import load_source_configs
from src.pipeline.credentials import load_warehouse_credentials
from src.pipeline.dbt_runner import set_dbt_env_vars, run_dbt
from src.pipeline.staging import run_stg_ingestion


def main():
    parser = argparse.ArgumentParser(description="Staging layer ingestion")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/table_config.csv"),
        help="Path to CSV config file",
    )
    parser.add_argument(
        "--secrets",
        type=Path,
        default=Path(".dlt/secrets.toml"),
        help="Path to secrets.toml with credentials",
    )
    parser.add_argument(
        "--bronze-url",
        type=str,
        default="data/bronze",
        help="Base directory of bronze parquet files",
    )
    parser.add_argument(
        "--dbt-dir",
        type=Path,
        default=Path("dbt"),
        help="Path to dbt project directory",
    )
    parser.add_argument(
        "--source",
        type=str,
        default=None,
        help="Run only a specific source by name (default: run all)",
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        default=7,
        help="Number of days to retain in stg_temp (default: 7)",
    )
    parser.add_argument(
        "--skip-dbt",
        action="store_true",
        help="Skip dbt run (only load parquet into stg_temp)",
    )
    args = parser.parse_args()

    sources = load_source_configs(args.config)
    warehouse_credentials = load_warehouse_credentials(args.secrets)

    for source_config in sources:
        if args.source and source_config.name != args.source:
            continue
        print(f"[stg_{source_config.name}] Loading parquet into stg_temp...")
        run_stg_ingestion(source_config, args.bronze_url, warehouse_credentials)

    if not args.skip_dbt:
        print("[stg] Running dbt models...")
        set_dbt_env_vars(warehouse_credentials)
        run_dbt(args.dbt_dir)


if __name__ == "__main__":
    main()
