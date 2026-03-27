"""Centralized credential loading from .dlt/secrets.toml."""

import tomllib
from pathlib import Path

from src.pipeline.settings import SECRETS_PATH


def _load_secrets_toml(secrets_path: Path = None) -> dict:
    """Parse the TOML secrets file and return the raw dict."""
    path = secrets_path or SECRETS_PATH
    if not path.exists():
        raise FileNotFoundError(
            f"Secrets file not found: {path}. "
            f"Copy .dlt/secrets.toml.example to .dlt/secrets.toml and fill in credentials."
        )
    with open(path, "rb") as f:
        return tomllib.load(f)


def load_source_credentials(source_name: str, secrets_path: Path = None) -> str:
    """Return the connection string for a named RDBMS source."""
    raw = _load_secrets_toml(secrets_path)
    try:
        return raw["sources"][source_name]["credentials"]
    except KeyError:
        raise ValueError(f"No credentials found for source '{source_name}' in secrets")


def load_warehouse_credentials(secrets_path: Path = None) -> str:
    """Return the connection string for the data warehouse."""
    raw = _load_secrets_toml(secrets_path)
    credentials = raw.get("destinations", {}).get("warehouse", {}).get("credentials")
    if not credentials:
        raise ValueError("No warehouse credentials found at [destinations.warehouse]")
    return credentials


def load_all_source_credentials(secrets_path: Path = None) -> dict[str, str]:
    """Return a dict of {source_name: connection_string} for all configured sources."""
    raw = _load_secrets_toml(secrets_path)
    secrets = {}
    for key, value in raw.get("sources", {}).items():
        if isinstance(value, dict) and "credentials" in value:
            secrets[key] = value["credentials"]
    return secrets
