"""Generate thin DAG files from dag_config.csv — run as a standalone script."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.pipeline.dag_generator import run_generation
from src.pipeline.settings import DAG_CONFIG_PATH

DAGS_OUTPUT_DIR = Path(__file__).parent / "layer__data_subject__src"

if __name__ == "__main__":
    run_generation(config_path=DAG_CONFIG_PATH, output_dir=DAGS_OUTPUT_DIR)
