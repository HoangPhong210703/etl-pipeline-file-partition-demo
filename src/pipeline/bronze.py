import datetime
from pathlib import Path

import dlt
from dlt.destinations import filesystem
from dlt.sources.sql_database import sql_database
from sqlalchemy import create_engine, text

from src.pipeline.config import SourceConfig


def extract_row_counts(load_info) -> dict[str, int]:
    """Extract per-table row counts from a dlt LoadInfo object."""
    counts: dict[str, int] = {}
    try:
        for metrics_list in (load_info.metrics or {}).values():
            for metric in metrics_list:
                for table_name, table_metric in (metric.get("tables") or {}).items():
                    if table_name.startswith("_dlt"):
                        continue
                    rows = table_metric.get("rows_count", 0)
                    counts[table_name] = counts.get(table_name, 0) + rows
    except Exception:
        pass
    return counts


def _parse_date(value: str) -> datetime.datetime:
    """Parse a date string in ISO format (2024-01-01) or US format (1/1/2024)."""
    try:
        return datetime.datetime.fromisoformat(value)
    except ValueError:
        return datetime.datetime.strptime(value, "%m/%d/%Y")


def test_source_connection(credentials: str, schema: str) -> None:
    """Test that the source database is reachable. Raises on failure."""
    engine = create_engine(credentials)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    engine.dispose()
    print(f"[source_connection] Connection to schema '{schema}' OK")


def build_layout() -> str:
    return "{table_name}/{DD}-{MM}-{YYYY}.{ext}"


def build_bucket_url(base_url: str, source_config: SourceConfig, data_subject: str) -> str:
    return f"{base_url}/{data_subject}/{source_config.name}"


def build_pipeline(source_config: SourceConfig, bucket_url: str, data_subject: str) -> dlt.Pipeline:
    dest = filesystem(
        bucket_url=build_bucket_url(bucket_url, source_config, data_subject),
        layout=build_layout(),
    )

    return dlt.pipeline(
        pipeline_name=f"bronze_{source_config.name}_{data_subject}",
        destination=dest,
        dataset_name=source_config.schema,
    )


def _is_first_run(pipeline: dlt.Pipeline) -> bool:
    """Check if this pipeline has been run before by looking for existing state."""
    try:
        return pipeline.state.get("default_schema_name") is None
    except Exception:
        return True


def rotate_todays_parquet(bucket_url: str, source_config: SourceConfig) -> None:
    """If today's parquet exists, rename it with the next available suffix before the new run."""
    today = datetime.date.today().strftime("%d-%m-%Y")

    for table_config in source_config.tables:
        base = Path(build_bucket_url(bucket_url, source_config, table_config.data_subject))
        parquet_file = base / source_config.schema / table_config.name / f"{today}.parquet"
        if parquet_file.exists():
            suffix = 1
            while True:
                rotated = parquet_file.parent / f"{today}({suffix}).parquet"
                if not rotated.exists():
                    parquet_file.rename(rotated)
                    print(f"[{source_config.name}] Rotated {parquet_file.name} → {rotated.name}")
                    break
                suffix += 1



def extract_tables(
    source_config: SourceConfig,
    bucket_url: str,
    credentials: str,
    data_subject: str,
) -> dict[str, str]:
    """Fetch data from RDBMS and normalize (extract + normalize step of dlt).
    
    Extracts all tables together with retry logic (3 attempts for entire extraction).
    Returns dict: {table_name: status} where status is 'success' or error message.
    """
    import time
    
    table_configs = [t for t in source_config.tables if t.data_subject == data_subject]
    table_results = {}
    max_retries = 3
    
    # Retry loop: up to 3 attempts (entire extraction)
    for attempt in range(1, max_retries + 1):
        # Create fresh pipeline for each attempt to avoid state contamination
        pipeline = build_pipeline(source_config, bucket_url, data_subject)
        first_run = _is_first_run(pipeline)

        if attempt == 1:
            if first_run:
                print(f"[{source_config.name}/{data_subject}] First run — full load")
            else:
                print(f"[{source_config.name}/{data_subject}] Subsequent run — configured strategies")
            print(f"\n[{source_config.name}/{data_subject}] Extracting {len(table_configs)} tables:")
        
        try:
            # Create source for ALL tables together
            source = sql_database(
                credentials=credentials,
                schema=source_config.schema,
                table_names=[t.name for t in table_configs],
                backend="pyarrow",
            )
            
            # Apply incremental load strategy if needed
            for table_config in table_configs:
                if not first_run and table_config.load_strategy == "incremental" and table_config.cursor_column:
                    resource = source.resources[table_config.name]
                    initial_value = table_config.initial_value
                    if initial_value:
                        initial_value = _parse_date(initial_value)
                    resource.apply_hints(
                        incremental=dlt.sources.incremental(
                            table_config.cursor_column,
                            initial_value=initial_value,
                        ),
                    )
            
            # Extract and normalize ALL tables at once (atomic operation)
            write_disp = "replace" if first_run else "append"
            pipeline.extract(source, write_disposition=write_disp, loader_file_format="parquet")
            pipeline.normalize()
            
            # Log success for each table
            for table_config in table_configs:
                print(f"  ✓ {table_config.name}: SUCCESS")
                table_results[table_config.name] = "success"
            
            break  # Success, exit retry loop
                
        except Exception as e:
            error_msg = f"{type(e).__name__}: {str(e)[:80]}"
            if attempt < max_retries:
                print(f"  ⟳ Attempt {attempt}/{max_retries} failed, retrying in 5s... ({error_msg})")
                time.sleep(5)  # Wait 5 seconds before retry
            else:
                print(f"  ✗ Extraction FAILED after {max_retries} attempts — {error_msg}")
                for table_config in table_configs:
                    table_results[table_config.name] = error_msg
    
    # Check if all tables succeeded
    failed_tables = [t for t, status in table_results.items() if status != "success"]
    
    print(f"[{source_config.name}/{data_subject}] Extract + normalize complete")
    print(f"[{source_config.name}/{data_subject}] Result: {len(table_configs) - len(failed_tables)} success, {len(failed_tables)} failed\n")
    
    # If any table failed, fail the entire extraction
    if failed_tables:
        error_detail = "\n".join([f"  • {t}: {table_results[t]}" for t in failed_tables])
        raise RuntimeError(
            f"Extraction failed for {len(failed_tables)} table(s) in {source_config.name}__{data_subject}:\n{error_detail}\n"
            f"All tables must succeed. Aborting extraction."
        )
    
    return table_results


def load_to_parquet(
    source_config: SourceConfig,
    bucket_url: str,
    data_subject: str,
) -> dict[str, int]:
    """Write normalized data to parquet files (load step of dlt).

    Returns dict of {table_name: row_count}.
    """
    pipeline = build_pipeline(source_config, bucket_url, data_subject)
    load_info = pipeline.load()
    counts = extract_row_counts(load_info)
    print(f"[{source_config.name}/{data_subject}] Write parquet complete: {load_info}")
    if counts:
        print(f"[{source_config.name}/{data_subject}] Row counts: {counts}")
    return counts


