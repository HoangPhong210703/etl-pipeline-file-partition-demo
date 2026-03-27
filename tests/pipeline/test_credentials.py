import pytest
from pathlib import Path

from src.pipeline.credentials import (
    load_source_credentials,
    load_warehouse_credentials,
    load_all_source_credentials,
)

SAMPLE_TOML = """\
[sources.postgres_crm]
credentials = "postgresql://user:pass@localhost:5432/crm"

[sources.maria_erp]
credentials = "mysql://user:pass@localhost:3306/erp"

[destinations.warehouse]
credentials = "postgresql://wh_user:wh_pass@localhost:5432/warehouse"
"""


@pytest.fixture
def secrets_file(tmp_path):
    p = tmp_path / "secrets.toml"
    p.write_text(SAMPLE_TOML)
    return p


def test_load_source_credentials(secrets_file):
    creds = load_source_credentials("postgres_crm", secrets_path=secrets_file)
    assert creds == "postgresql://user:pass@localhost:5432/crm"


def test_load_source_credentials_missing_source(secrets_file):
    with pytest.raises(ValueError, match="No credentials found for source"):
        load_source_credentials("nonexistent", secrets_path=secrets_file)


def test_load_warehouse_credentials(secrets_file):
    creds = load_warehouse_credentials(secrets_path=secrets_file)
    assert creds == "postgresql://wh_user:wh_pass@localhost:5432/warehouse"


def test_load_warehouse_credentials_missing(tmp_path):
    p = tmp_path / "secrets.toml"
    p.write_text("[sources.foo]\ncredentials = 'bar'\n")
    with pytest.raises(ValueError, match="No warehouse credentials"):
        load_warehouse_credentials(secrets_path=p)


def test_load_all_source_credentials(secrets_file):
    creds = load_all_source_credentials(secrets_path=secrets_file)
    assert creds == {
        "postgres_crm": "postgresql://user:pass@localhost:5432/crm",
        "maria_erp": "mysql://user:pass@localhost:3306/erp",
    }


def test_missing_secrets_file(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_source_credentials("any", secrets_path=tmp_path / "missing.toml")
