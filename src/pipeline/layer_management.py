"""Layer management — reads layer_management_config.csv to determine
whether to auto-trigger the next layer after the current one completes."""

import csv

from src.pipeline.settings import LAYER_MANAGEMENT_CONFIG_PATH as LAYER_MGMT_PATH


def get_next_layer(current_layer: str, data_subject: str, source: str) -> str | None:
    """Return the target layer's button dag_id if auto_trigger is enabled, else None."""
    if not LAYER_MGMT_PATH.exists():
        return None

    with open(LAYER_MGMT_PATH) as f:
        for row in csv.DictReader(f):
            if (
                row["source_layer"] == current_layer
                and row["data_subject"] == data_subject
                and row["source"] == source
                and row.get("active", "1").strip() in ("1", "TRUE", "true")
                and row.get("auto_trigger", "0").strip() in ("1", "TRUE", "true")
            ):
                target_layer = row["target_layer"]
                return f"{target_layer}__{data_subject}__{source}"

    return None


def get_stg_datasets() -> list[dict]:
    """Return dataset identifiers for brz2sil → stg2sil triggers.

    Returns list of dicts: [{"data_subject": ..., "source": ...}, ...]
    The caller wraps these into Airflow Dataset objects.
    """
    datasets = []
    if not LAYER_MGMT_PATH.exists():
        return datasets
    with open(LAYER_MGMT_PATH) as f:
        for row in csv.DictReader(f):
            if (
                row["source_layer"] == "brz2sil"
                and row["target_layer"] == "stg2sil"
                and row.get("active", "1").strip() in ("1", "TRUE", "true")
            ):
                datasets.append({
                    "data_subject": row["data_subject"],
                    "source": row["source"],
                })
    return datasets
