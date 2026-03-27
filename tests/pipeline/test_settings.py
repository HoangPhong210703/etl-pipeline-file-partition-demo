import sys
from pathlib import Path, PurePosixPath

from src.pipeline.settings import (
    SECRETS_PATH,
    BRONZE_BASE_URL,
    CONFIG_DIR,
    AUDIT_LOG_DIR,
    DBT_PROJECT_DIR,
)


def test_all_paths_are_path_objects():
    assert isinstance(SECRETS_PATH, Path)
    assert isinstance(CONFIG_DIR, Path)
    assert isinstance(AUDIT_LOG_DIR, Path)
    assert isinstance(DBT_PROJECT_DIR, Path)


def test_bronze_base_url_is_string():
    assert isinstance(BRONZE_BASE_URL, str)


def test_paths_are_absolute():
    # On Windows, /opt/airflow is not absolute per WindowsPath.
    # Use PurePosixPath with forward slashes since these run in Linux containers.
    for p in (SECRETS_PATH, CONFIG_DIR, AUDIT_LOG_DIR, DBT_PROJECT_DIR):
        if sys.platform == "win32":
            assert PurePosixPath(str(p).replace("\\", "/")).is_absolute()
        else:
            assert p.is_absolute()
