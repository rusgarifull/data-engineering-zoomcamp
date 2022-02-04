from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
import logging

def get_tomorrows_date(ds=None):
    today = ds
    tomorrow_date = datetime.strftime((datetime.strptime(ds, "%Y-%m-%d") + timedelta(1)), "%Y-%m-%d") 
    print(f'today is {today}, \n tomorrow is {tomorrow_date}')
    logging.warning("This is a warning message")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2021, 1, 1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="test_workflow",
    schedule_interval="0 6 2 * *",
    default_args=default_args,
) as dag:

    wget_task = BashOperator(
        task_id="wget",
        bash_command="echo hello world"
    )

    ingest_task = BashOperator(
        task_id="ingest_task",
        bash_command="echo this is ingest task"
    )

    print_task = PythonOperator(
        task_id='print_taks',
        python_callable=get_tomorrows_date
    )
    wget_task >> ingest_task >> print_task