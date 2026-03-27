"""CLI entry point for bronze layer ingestion (RDBMS → Parquet)."""

import argparse
from pathlib import Path

from src.pipeline.config import load_source_configs
from src.pipeline.bronze import extract_tables, load_to_parquet
from src.pipeline.credentials import load_all_source_credentials


def main():
    parser = argparse.ArgumentParser(description="Bronze layer ingestion")
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
        help="Path to secrets.toml with database credentials",
    )
    parser.add_argument(
        "--bucket-url",
        type=str,
        default="data/bronze",
        help="Output directory for parquet files",
    )
    parser.add_argument(
        "--source",
        type=str,
        default=None,
        help="Run only a specific source by name (default: run all)",
    )
    args = parser.parse_args()

    sources = load_source_configs(args.config)
    secrets = load_all_source_credentials(args.secrets)

    for source_config in sources:
        if args.source and source_config.name != args.source:
            continue

        credentials = secrets.get(source_config.name)
        if not credentials:
            print(f"[{source_config.name}] Skipping — no credentials in secrets file")
            continue

        print(f"[{source_config.name}] Starting ingestion...")
        for data_subject in sorted({t.data_subject for t in source_config.tables}):
            extract_tables(source_config, args.bucket_url, credentials, data_subject)
            load_to_parquet(source_config, args.bucket_url, data_subject)


if __name__ == "__main__":
    main()
