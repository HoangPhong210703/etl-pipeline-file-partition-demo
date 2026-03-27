"""dbt subprocess wrappers — run models, snapshots, tests, and parse results."""

import json
import os
import subprocess
from pathlib import Path
from urllib.parse import urlparse


def set_dbt_env_vars(credentials: str) -> None:
    """Parse connection string and set env vars for dbt profiles.yml."""
    parsed = urlparse(credentials)
    os.environ["WAREHOUSE_HOST"] = parsed.hostname or "localhost"
    os.environ["WAREHOUSE_PORT"] = str(parsed.port or 5432)
    os.environ["WAREHOUSE_USER"] = parsed.username or ""
    os.environ["WAREHOUSE_PASSWORD"] = parsed.password or ""
    os.environ["WAREHOUSE_DB"] = (parsed.path or "").lstrip("/")


def run_dbt(dbt_project_dir: Path, selectors: list[str] | None = None) -> None:
    """Run dbt models. If selectors is None, runs stg and silver."""
    if selectors is None:
        selectors = ["stg", "silver"]
    profiles_dir = dbt_project_dir
    for selector in selectors:
        result = subprocess.run(
            ["dbt", "run", "--select", selector, "--profiles-dir", str(profiles_dir)],
            cwd=str(dbt_project_dir),
            capture_output=True,
            text=True,
        )
        print(result.stdout)
        if result.returncode != 0:
            print(result.stderr)
            raise RuntimeError(f"dbt run --select {selector} failed with exit code {result.returncode}")


def run_dbt_snapshot(dbt_project_dir: Path) -> None:
    """Run dbt snapshots (SCD-2 tables)."""
    result = subprocess.run(
        ["dbt", "snapshot", "--profiles-dir", str(dbt_project_dir)],
        cwd=str(dbt_project_dir),
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"dbt snapshot failed with exit code {result.returncode}")


def run_dbt_test(dbt_project_dir: Path) -> None:
    """Run dbt tests. Logs warnings but does not raise on failure."""
    result = subprocess.run(
        ["dbt", "test", "--profiles-dir", str(dbt_project_dir)],
        cwd=str(dbt_project_dir),
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(f"[dbt_test] WARNING: dbt test returned exit code {result.returncode}")
        print(result.stderr)


def parse_dbt_results(dbt_project_dir: Path) -> list[dict]:
    """Parse dbt target/run_results.json and return structured test results."""
    results_path = dbt_project_dir / "target" / "run_results.json"
    if not results_path.exists():
        print(f"[dbt] No run_results.json found at {results_path}")
        return []

    with open(results_path) as f:
        data = json.load(f)

    return [
        {
            "test_name": r.get("unique_id", "unknown"),
            "status": r.get("status", "unknown"),
            "failures": r.get("failures", 0) or 0,
            "execution_time": r.get("execution_time", 0) or 0,
            "message": r.get("message"),
        }
        for r in data.get("results", [])
    ]
