"""Data freshness checking — queries warehouse for stale sources and alerts."""

import csv
from datetime import datetime, timezone
from pathlib import Path

from src.pipeline.credentials import load_warehouse_credentials
from src.pipeline.settings import FRESHNESS_CONFIG_PATH


def load_freshness_thresholds(config_path: Path = None) -> list[dict]:
    """Read freshness_config.csv and return active threshold configurations."""
    path = config_path or FRESHNESS_CONFIG_PATH
    if not path.exists():
        return []
    thresholds = []
    with open(path) as f:
        for row in csv.DictReader(f):
            if row.get("active", "1").strip() == "1":
                thresholds.append({
                    "source_name": row["source_name"],
                    "data_subject": row["data_subject"],
                    "max_stale_hours": int(row["max_stale_hours"]),
                })
    return thresholds


def check_freshness(warehouse_credentials: str = None) -> list[dict]:
    """Query warehouse for last successful run per source, evaluate staleness.

    Returns list of freshness result dicts. Sends alerts for stale sources.
    """
    from sqlalchemy import create_engine, text
    from src.pipeline.alert import send_alert
    from src.pipeline.audit.db_logger import log_freshness_results
    from src.pipeline.audit.file_logger import log_freshness_to_file

    thresholds = load_freshness_thresholds()
    if not thresholds:
        print("[freshness] No active freshness checks configured")
        return []

    credentials = warehouse_credentials or load_warehouse_credentials()
    engine = create_engine(credentials)

    stale_sources = []
    freshness_results = []
    now = datetime.now(timezone.utc)

    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = 'meta' AND table_name = 'pipeline_audit')"
        ))
        if not result.scalar():
            print("[freshness] meta.pipeline_audit table does not exist yet, skipping")
            engine.dispose()
            return []

        for t in thresholds:
            result = conn.execute(
                text(
                    "SELECT MAX(finished_at) as last_success "
                    "FROM meta.pipeline_audit "
                    "WHERE source = :source AND data_subject = :subject "
                    "AND status = 'success' "
                    "AND dag_id LIKE 'src2brz%'"
                ),
                {"source": t["source_name"], "subject": t["data_subject"]},
            )
            row = result.fetchone()
            last_success = row[0] if row else None

            if last_success is None:
                status = "stale"
                hours_ago = None
                stale_sources.append(f"  {t['source_name']}/{t['data_subject']}: never loaded")
                print(f"[freshness] {t['source_name']}/{t['data_subject']}: NEVER loaded")
            else:
                delta = now - last_success
                hours_ago = round(delta.total_seconds() / 3600, 1)
                if hours_ago > t["max_stale_hours"]:
                    status = "stale"
                    stale_sources.append(
                        f"  {t['source_name']}/{t['data_subject']}: "
                        f"last loaded {hours_ago:.1f}h ago (threshold: {t['max_stale_hours']}h)"
                    )
                    print(f"[freshness] {t['source_name']}/{t['data_subject']}: STALE ({hours_ago:.1f}h)")
                else:
                    status = "fresh"
                    print(f"[freshness] {t['source_name']}/{t['data_subject']}: OK ({hours_ago:.1f}h)")

            freshness_results.append({
                "source_name": t["source_name"],
                "data_subject": t["data_subject"],
                "status": status,
                "max_stale_hours": t["max_stale_hours"],
                "hours_since_load": hours_ago,
                "last_loaded_at": last_success,
            })

    engine.dispose()

    try:
        log_freshness_results(freshness_results)
    except Exception as e:
        print(f"[freshness] WARNING: Failed to log freshness to DB: {e}")

    log_freshness_to_file(freshness_results)

    if stale_sources:
        send_alert(
            alert_type="data_freshness",
            subject=f"Stale data detected: {len(stale_sources)} source(s)",
            body="The following sources have not been loaded within their freshness threshold:\n\n"
                 + "\n".join(stale_sources),
        )
        print(f"[freshness] {len(stale_sources)} stale source(s) detected, alert sent")
    else:
        print(f"[freshness] All {len(thresholds)} sources are fresh")

    return freshness_results
