from datetime import datetime

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id="src2brz_accounting_timesheet",
    description="Button: trigger source-to-bronze for accounting from timesheet",
    schedule=None, # Nút bấm nên không cần lịch trình cố định
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["button", "src2brz", "accounting", "postgres_timesheet"],
) as dag:
    
    # Kích hoạt trực tiếp DAG coordinator
    trigger_coor = TriggerDagRunOperator(
        task_id="trigger_coordinator",
        trigger_dag_id="coordinator",
        conf={"data_subject": "accounting", "source": "postgres_timesheet"}, # Báo cho coordinator biết đang gọi
        wait_for_completion=False, # True nếu muốn nút bấm chờ coordinator chạy xong
    )
