"""DAG file generation — reads dag_config.csv and writes thin DAG .py files."""

import csv
import logging
from datetime import datetime
from pathlib import Path

from src.pipeline.settings import DAG_CONFIG_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# --- DAG File Template ---
DAG_TEMPLATE = """from datetime import datetime

from airflow import DAG  # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # type: ignore

DAG_ID = "{dag_id}"

with DAG(
    dag_id=DAG_ID,
    description="{description}",
    schedule={schedule_interval},
    start_date=datetime({start_date}),
    catchup={catchup},
    tags={tags},
    max_active_runs={max_active_runs},
) as dag:

    trigger_coor = TriggerDagRunOperator(
        task_id="trigger_coordinator",
        trigger_dag_id="{trigger_target}",
        conf={{"button": DAG_ID}},
        wait_for_completion=False,
    )
"""


def clear_existing_dags(output_dir: Path) -> None:
    """Remove existing auto-generated DAG files from output_dir."""
    logging.info(f"Clearing existing DAGs in {output_dir}...")
    for file_path in output_dir.glob("*.py"):
        if "__" in file_path.stem and file_path.name != "__init__.py":
            logging.info(f"Removing {file_path.name}")
            file_path.unlink()


def generate_dags(config_path: Path = None, output_dir: Path = None) -> None:
    """Read dag_config.csv and generate DAG files from template."""
    path = config_path or DAG_CONFIG_PATH
    if not path.exists():
        logging.error(f"Config file not found at {path}")
        return

    logging.info(f"Reading DAG configuration from {path}")

    with open(path, mode="r", encoding="utf-8") as infile:
        reader = csv.DictReader(infile)

        for row in reader:
            if row.get("dag_active", "0").strip() != "1":
                logging.info(f"Skipping inactive DAG: {row.get('layer__data_subject__src')}")
                continue

            try:
                dag_id = row["layer__data_subject__src"]
                tags_list = [tag.strip() for tag in row["tags"].split(",")]

                schedule_interval = row["schedule_interval"]
                if schedule_interval.lower() == "none":
                    schedule = "None"
                else:
                    schedule = f"'{schedule_interval}'"

                start_date_dt = datetime.strptime(row["start_date"], "%Y-%m-%d")
                start_date_str = f"{start_date_dt.year}, {start_date_dt.month}, {start_date_dt.day}"

                catchup_bool = row["catchup"].strip().lower() == "true"

                content = DAG_TEMPLATE.format(
                    dag_id=dag_id,
                    description=row["description"].replace('"', '\\"'),
                    schedule_interval=schedule,
                    start_date=start_date_str,
                    catchup=catchup_bool,
                    tags=tags_list,
                    max_active_runs=int(row["max_active_runs"]),
                    trigger_target=row["trigger_target"],
                )

                out = output_dir or Path(".")
                output_path = out / f"{dag_id}.py"
                with open(output_path, "w", encoding="utf-8") as outfile:
                    outfile.write(content)
                logging.info(f"Generated DAG: {output_path.name}")

            except KeyError as e:
                logging.error(f"Skipping row due to missing key: {e}. Row: {row}")
            except Exception as e:
                logging.error(f"An error occurred while processing row {row.get('id')}: {e}")


def run_generation(config_path: Path = None, output_dir: Path = None) -> None:
    """Full generation run: create output dir, clear old DAGs, generate new ones."""
    out = output_dir or Path(".")
    out.mkdir(parents=True, exist_ok=True)

    logging.info("--- Starting DAG Generation Script ---")
    clear_existing_dags(out)
    generate_dags(config_path, out)
    logging.info("--- DAG Generation Script Finished ---")
