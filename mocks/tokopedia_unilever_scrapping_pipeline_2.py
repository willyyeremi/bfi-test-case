from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule

import datetime
import sys

sys.path.insert(0, '/opt/airflow/dags/tokopedia_unilever_scrapping_pipeline')
import module

credential_path = '/home/airflow/credential.csv'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2023, 7, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG('bfi_test_case', default_args = default_args, schedule_interval = None) as dag:
    def run_firefox_script_func():
        module.scrapper.run_firefox_script()

    def product_list_loader_task_func(**kwargs):
        new_url = kwargs['ti'].xcom_pull(task_ids = 'product_list_loader_task', key = 'new_url')
        url = new_url if new_url else 'https://www.tokopedia.com/unilever/product'
        link_list, new_url, new_state = module.scrapper.product_list_loader(url)
        kwargs['ti'].xcom_push(key = 'link_list', value = link_list)
        kwargs['ti'].xcom_push(key = 'new_url', value = new_url)
        kwargs['ti'].xcom_push(key = 'new_state', value = new_state)

    def web_data_get_task_func(**kwargs):
        link_list = kwargs['ti'].xcom_pull(task_ids = 'product_list_loader_task', key = 'link_list')
        data_list = module.scrapper.web_data_get(link_list)
        kwargs['ti'].xcom_push(key = 'data_list', value = data_list)

    def product_master_input_task_func(**kwargs):
        data_list = kwargs['ti'].xcom_pull(task_ids = 'web_data_get_task', key = 'data_list')
        module.scrapper.product_master_input(credential_path, data_list)

    def product_input_task_func(**kwargs):
        data_list = kwargs['ti'].xcom_pull(task_ids = 'web_data_get_task', key = 'data_list')
        module.scrapper.product_input(credential_path, data_list)
    
    run_firefox_task = PythonOperator(
        task_id = 'run_firefox_task',
        python_callable = run_firefox_script_func,
        dag = dag
    )

    product_list_loader_task = PythonOperator(
        task_id = 'product_list_loader_task',
        python_callable = product_list_loader_task_func,
        provide_context = True,
        dag = dag
    )

    web_data_get_task = PythonOperator(
        task_id = 'web_data_get_task',
        python_callable = web_data_get_task_func,
        provide_context = True,
        dag = dag
    )
    
    product_master_input_task = PythonOperator(
        task_id = 'product_master_input_task',
        python_callable = product_master_input_task_func,
        provide_context = True,
        dag = dag
    )

    product_input_task = PythonOperator(
        task_id = 'product_input_task',
        python_callable = product_input_task_func,
        provide_context = True,
        dag = dag
    )

    run_firefox_task >> product_list_loader_task >> web_data_get_task >> product_master_input_task >> product_input_task