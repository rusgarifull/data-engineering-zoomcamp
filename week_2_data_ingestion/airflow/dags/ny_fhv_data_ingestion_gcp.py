import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from utils import upload_to_gcs, format_to_parquet


# airflow metadata
DAG_ID = "ny_fhv_data_ingestion_gcp"

# GCS location
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
GCS_FOLDER = 'taxi_zones'

# SOURCE dataset
DATASET_FILE = "fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv"
DATASET_URL = f"https://nyc-tlc.s3.amazonaws.com/trip+data/{DATASET_FILE}"

# othe metadata
PATH_TO_LOCAL_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
PARQUET_FILE = DATASET_FILE.replace('.csv', '.parquet')


default_args = {
    "owner": "airflow",
    "start_date": datetime(2019, 1, 1),
    "end_date": datetime(2019, 2, 1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id=DAG_ID,
    schedule_interval="@monthly",
    default_args=default_args,
    catchup=True,
    max_active_runs=2,
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sS {DATASET_URL} > {PATH_TO_LOCAL_HOME}/{DATASET_FILE}"
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{PATH_TO_LOCAL_HOME}/{DATASET_FILE}",
        },
    )

    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{GCS_FOLDER}/{PARQUET_FILE}",
            "local_file": f"{PATH_TO_LOCAL_HOME}/{PARQUET_FILE}",
        },
    )

    remove_temp_file_task = BashOperator(
        task_id="remove_temp_file_task",
        bash_command=f"rm {PATH_TO_LOCAL_HOME}/{DATASET_FILE} {PATH_TO_LOCAL_HOME}/{PARQUET_FILE}"
    )

    download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> remove_temp_file_task
