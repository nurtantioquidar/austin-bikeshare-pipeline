from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
)
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

# Import functions from previous tasks
from scripts.extract import main as extract_data
from scripts.manage_biglake_table import manage_biglake_table
from google.oauth2 import service_account
import os
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

credentials = service_account.Credentials.from_service_account_file(
    os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "bikeshare_etl_pipeline",
    default_args=default_args,
    description="A DAG for Bikeshare ETL pipeline",
    schedule_interval=timedelta(days=1),
)


def extract_data_task(**kwargs):
    extract_date = (kwargs["execution_date"] - timedelta(days=200)).strftime("%Y-%m-%d")
    config = {
        "bucket_name": kwargs["dag_run"].conf.get(
            "bucket_name", os.environ.get("GCP_GCS_BUCKET")
        ),
        "date": extract_date,
        "days_ago": kwargs["dag_run"].conf.get("days_ago", 200),
        "limit": kwargs["dag_run"].conf.get("limit", 1000),
    }
    extract_data(config)


extract_task = PythonOperator(
    task_id="extract_data",
    python_callable=extract_data_task,
    provide_context=True,
    op_kwargs={"credentials": credentials},
    dag=dag,
)


def create_biglake_table_task(**kwargs):
    result = manage_biglake_table(
        project_id=os.environ.get("GCP_PROJECT_ID"),
        dataset_id=os.environ.get("GCP_DATASET_ID"),
        table_id=os.environ.get("GCP_TABLE_ID"),
        gcs_bucket=os.environ.get("GCP_GCS_BUCKET"),
        gcs_path=os.environ.get("GCP_GCS_PATH"),
    )
    # Log the result
    logging.info(f"BigLake table managed: {result}")
    return result


create_biglake_table_task = PythonOperator(
    task_id="create_biglake_table",
    python_callable=create_biglake_table_task,
    op_kwargs={"credentials": credentials},
    dag=dag,
)

extract_task >> create_biglake_table_task
