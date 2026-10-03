from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def print_hello():
    print("Hello from Airflow! Your setup is working successfully.")
    return "Success"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="hello_world_test",
    default_args=default_args,
    description="A simple test DAG to verify Airflow functionality",
    schedule=None,  # Updated from schedule_interval for Airflow 3 compatibility
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["test", "quickstart"],
) as dag:

    hello_task = PythonOperator(
        task_id="print_hello_task",
        python_callable=print_hello,
    )

    hello_task