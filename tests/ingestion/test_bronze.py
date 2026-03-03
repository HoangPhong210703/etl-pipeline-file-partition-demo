import pytest
from src.ingestion.config import SourceConfig, TableConfig
from src.ingestion.bronze import build_pipeline, build_layout, build_bucket_url


@pytest.fixture
def source_config():
    return SourceConfig(
        name="test_pg",
        data_subject="crm",
        schema="public",
        tables=[
            TableConfig(name="customers", load_strategy="full"),
            TableConfig(
                name="orders",
                load_strategy="incremental",
                cursor_column="updated_at",
                initial_value="2024-01-01",
            ),
        ],
    )


def test_build_layout():
    layout = build_layout()
    assert "{table_name}" in layout
    assert "{DD}" in layout
    assert "{MM}" in layout
    assert "{YYYY}" in layout
    assert layout.endswith(".{ext}")


def test_build_bucket_url(source_config):
    url = build_bucket_url("data/bronze", source_config)
    assert url == "data/bronze/crm/test_pg"


def test_build_pipeline_returns_dlt_pipeline(source_config):
    pipeline = build_pipeline(source_config, bucket_url="/tmp/test_bronze")
    assert pipeline.pipeline_name == "bronze_test_pg"
    assert pipeline.destination.destination_name == "filesystem"
