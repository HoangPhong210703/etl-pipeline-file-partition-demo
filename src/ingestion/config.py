from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import yaml


@dataclass
class TableConfig:
    name: str
    load_strategy: str  # "full" or "incremental"
    cursor_column: Optional[str] = None
    initial_value: Optional[str] = None


@dataclass
class SourceConfig:
    name: str
    data_subject: str
    schema: str
    tables: list[TableConfig]


_REQUIRED_SOURCE_FIELDS = ("name", "data_subject", "schema", "tables")
_REQUIRED_TABLE_FIELDS = ("name", "load_strategy")


def load_sources_config(config_path: Path) -> list[SourceConfig]:
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r") as f:
        raw = yaml.safe_load(f)

    sources = []
    for raw_source in raw.get("sources", []):
        for field in _REQUIRED_SOURCE_FIELDS:
            if field not in raw_source:
                raise ValueError(
                    f"Missing required field '{field}' in source config: {raw_source}"
                )

        tables = []
        for raw_table in raw_source.get("tables", []):
            for field in _REQUIRED_TABLE_FIELDS:
                if field not in raw_table:
                    raise ValueError(
                        f"Missing required field '{field}' in table config: {raw_table}"
                    )
            tables.append(
                TableConfig(
                    name=raw_table["name"],
                    load_strategy=raw_table["load_strategy"],
                    cursor_column=raw_table.get("cursor_column"),
                    initial_value=raw_table.get("initial_value"),
                )
            )

        sources.append(
            SourceConfig(
                name=raw_source["name"],
                data_subject=raw_source["data_subject"],
                schema=raw_source["schema"],
                tables=tables,
            )
        )

    return sources
