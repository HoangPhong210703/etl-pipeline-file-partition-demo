from pathlib import Path

import dlt
from dlt.destinations import filesystem
from dlt.sources.sql_database import sql_database

from src.ingestion.config import SourceConfig, load_sources_config


def build_layout() -> str:
    return "{table_name}/{DD}-{MM}-{YYYY}.{ext}"


def build_bucket_url(base_url: str, source_config: SourceConfig) -> str:
    return f"{base_url}/{source_config.data_subject}/{source_config.name}"


def build_pipeline(source_config: SourceConfig, bucket_url: str) -> dlt.Pipeline:
    dest = filesystem(
        bucket_url=build_bucket_url(bucket_url, source_config),
        layout=build_layout(),
    )

    return dlt.pipeline(
        pipeline_name=f"bronze_{source_config.name}",
        destination=dest,
        dataset_name=source_config.schema,
    )


def run_source_ingestion(
    source_config: SourceConfig,
    bucket_url: str,
    credentials: str,
) -> None:
    pipeline = build_pipeline(source_config, bucket_url)

    table_names = [t.name for t in source_config.tables]
    source = sql_database(
        credentials=credentials,
        schema=source_config.schema,
        table_names=table_names,
        backend="pyarrow",
    )

    for table_config in source_config.tables:
        if table_config.load_strategy == "incremental" and table_config.cursor_column:
            resource = source.resources[table_config.name]
            resource.apply_hints(
                incremental=dlt.sources.incremental(
                    table_config.cursor_column,
                    initial_value=table_config.initial_value,
                ),
            )

    load_info = pipeline.run(source, write_disposition="append", loader_file_format="parquet")
    print(f"[{source_config.name}] Load complete: {load_info}")


def run_all_sources(config_path: Path, bucket_url: str, secrets: dict[str, str]) -> None:
    sources = load_sources_config(config_path)

    for source_config in sources:
        credentials = secrets.get(source_config.name)
        if not credentials:
            print(f"[{source_config.name}] Skipping — no credentials found")
            continue

        run_source_ingestion(source_config, bucket_url, credentials)
