from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from main import main
from settings import settings


default_args = {
    "owner": "Mohamed",
    "depends_on_past": False,
    "start_date": datetime(2023, 2, 23),
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
    "execution_timeout": timedelta(hours=12),
}

dag = DAG(
    "1.nz_scraping_dag",
    default_args=default_args,
    schedule_interval="0 5 * * *",
)


with dag:
    accor_brand_update_feed = PythonOperator(
        task_id="scraping_task",
        python_callable=main,
    )