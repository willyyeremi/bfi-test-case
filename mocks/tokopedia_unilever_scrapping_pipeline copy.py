from airflow import DAG
from airflow.operators.bash import BashOperator

import datetime
import sys


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2023, 7, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}


with DAG('bfi_test_case', default_args = default_args, schedule_interval = None, catchup = False) as dag:

    pricerecommendation_ml_task = BashOperator(
        task_id = 'pricerecommendation_ml_task',
        bash_command = 'python /opt/airflow/dags/tokopedia_unilever_scrapping_pipeline/pricerecommendation.py',
        dag = dag,
    )

    pricerecommendation_ml_task

