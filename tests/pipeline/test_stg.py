from src.pipeline.staging import (
    get_parquet_dir,
    get_latest_parquet_file,
    build_stg_pipeline,
)


def test_get_parquet_dir():
    path = get_parquet_dir(
        bronze_base_url="data/bronze",
        data_subject="crm",
        source_name="postgres_crm",
        schema="public",
        table_name="project_task",
    )
    assert path == "data/bronze/crm/postgres_crm/public/project_task"


def test_build_stg_pipeline():
    pipeline = build_stg_pipeline(
        source_name="postgres_crm",
        data_subject="accounting",
        warehouse_credentials="postgresql://user:pass@localhost:5432/test_db",
    )
    assert pipeline.pipeline_name == "stg_postgres_crm_accounting"
    assert pipeline.destination.destination_name == "postgres"
    assert pipeline.dataset_name == "stg__accounting__postgres_crm"


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
