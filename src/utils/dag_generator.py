import csv
from pathlib import Path
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Constants ---
# Use relative paths from the project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
CONFIG_PATH = Path("/opt/airflow/config/dag_config.csv")
DAGS_OUTPUT_DIR = PROJECT_ROOT / "dags" / "layer__data_subject__src"

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

def clear_existing_dags():
    """Removes existing auto-generated DAG files from the output directory."""
    logging.info(f"Clearing existing DAGs in {DAGS_OUTPUT_DIR}...")
    for file_path in DAGS_OUTPUT_DIR.glob("*.py"):
        # A simple check to avoid deleting __init__.py or other non-generated files
        if "__" in file_path.stem and file_path.name != "__init__.py":
            logging.info(f"Removing {file_path.name}")
            file_path.unlink()

def generate_dags():
    """Reads the config file and generates DAGs based on the template."""
    if not CONFIG_PATH.exists():
        logging.error(f"Config file not found at {CONFIG_PATH}")
        return

    logging.info(f"Reading DAG configuration from {CONFIG_PATH}")
    
    with open(CONFIG_PATH, mode='r', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        
        for row in reader:
            # Check if the DAG is marked as active
            if row.get('dag_active', '0').strip() != '1':
                logging.info(f"Skipping inactive DAG: {row.get('layer__data_subject__src')}")
                continue

            try:
                dag_id = row['layer__data_subject__src']
                tags_list = [tag.strip() for tag in row['tags'].split(',')]
                
                # Safely evaluate schedule_interval
                schedule_interval = row['schedule_interval']
                if schedule_interval.lower() == 'none':
                    schedule = 'None'
                else:
                    schedule = f"'{schedule_interval}'"

                # Format start_date for datetime constructor
                start_date_dt = datetime.strptime(row['start_date'], '%Y-%m-%d')
                start_date_str = f"{start_date_dt.year}, {start_date_dt.month}, {start_date_dt.day}"

                # Format catchup
                catchup_bool = row['catchup'].strip().lower() == 'true'

                # Generate DAG file content
                content = DAG_TEMPLATE.format(
                    dag_id=dag_id,
                    description=row['description'].replace('"', '\\"'),
                    schedule_interval=schedule,
                    start_date=start_date_str,
                    catchup=catchup_bool,
                    tags=tags_list,
                    max_active_runs=int(row['max_active_runs']),
                    trigger_target=row['trigger_target']
                )

                # Write the DAG file
                output_path = DAGS_OUTPUT_DIR / f"{dag_id}.py"
                with open(output_path, "w", encoding="utf-8") as outfile:
                    outfile.write(content)
                logging.info(f"Generated DAG: {output_path.name}")

            except KeyError as e:
                logging.error(f"Skipping row due to missing key: {e}. Row: {row}")
            except Exception as e:
                logging.error(f"An error occurred while processing row {row.get('id')}: {e}")

if __name__ == "__main__":
    logging.info("--- Starting DAG Generation Script ---")
    
    # Create the output directory if it doesn't exist
    DAGS_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Clear old DAGs before generating new ones
    clear_existing_dags()
    
    # Generate the new DAGs
    generate_dags()
    
    logging.info("--- DAG Generation Script Finished ---")
