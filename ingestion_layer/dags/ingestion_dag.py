from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# This line helps Airflow find your script
# It assumes your 'data_generators' folder is in a directory accessible to Airflow
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import the specific function you want to run
from data_generators.external_data_collector import create_external_data_files

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    'external_data_collector_dag',
    default_args=default_args,
    description='A DAG to collect external promotions data daily.',
    schedule_interval=timedelta(days=1), # This makes it run once a day
    catchup=False,
) as dag:

    # This is the task that will execute your function
    collect_external_data_task = PythonOperator(
        task_id='collect_external_promotions_data',
        python_callable=create_external_data_files,
    )