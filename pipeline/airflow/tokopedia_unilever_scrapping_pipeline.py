from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

import datetime
import sys
import ast

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

def run_firefox_script_func():
    module.scrapper.run_firefox_script()

def shop_page_list_collect_func(**kwargs):
    url = "https://www.tokopedia.com/unilever/product"
    dict_of_shop_page = module.scrapper.shop_page_list_collect(url)
    kwargs['ti'].xcom_push(key = 'dict_of_shop_page', value = dict_of_shop_page)

def product_list_loader_task_func(**kwargs):
    page_url_list = kwargs['page_url_list']
    page_url_list = ast.literal_eval(page_url_list)
    dict_of_product_page = module.scrapper.product_list_loader(page_url_list)
    kwargs['ti'].xcom_push(key = 'dict_of_product_page', value = dict_of_product_page)

def web_data_get_task_func(**kwargs):
    product_link_list = kwargs['product_link_list']
    product_link_list = ast.literal_eval(product_link_list)
    data_list = module.scrapper.web_data_get(product_link_list)
    kwargs['ti'].xcom_push(key = 'data_list', value = data_list)

def product_master_input_task_func(**kwargs):
    data_list = kwargs['data_list']
    module.scrapper.product_master_input(credential_path, data_list)

def product_input_task_func(**kwargs):
    data_list = kwargs['data_list']
    module.scrapper.product_input(credential_path, data_list)


with DAG('bfi_test_case', default_args = default_args, schedule_interval = None, catchup = False) as dag:

    with TaskGroup(group_id = 'scrapping_process', default_args = default_args) as scrapping_process:
        
        run_firefox_task = PythonOperator(
            task_id = 'run_firefox_task',
            python_callable = run_firefox_script_func
        )

        shop_page_list_collect_task = PythonOperator(
            task_id = 'shop_page_list_collect_task',
            python_callable = shop_page_list_collect_func
        )

        with TaskGroup(group_id = 'getting_product_link_1', default_args = default_args) as getting_product_link_1:

            product_list_loader_task = PythonOperator(
                task_id = 'product_list_loader_task',
                python_callable = product_list_loader_task_func,
                op_kwargs = {"page_url_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.shop_page_list_collect_task', key = 'dict_of_shop_page')['part_1']}}"}
            )

            with TaskGroup(group_id = 'getting_product_data_1', default_args = default_args) as getting_product_data_1:

                web_data_get_task = PythonOperator(
                    task_id = 'web_data_get_task',
                    python_callable = web_data_get_task_func,
                    op_kwargs = {"product_link_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_1.product_list_loader_task', key = 'dict_of_product_page')['part_1']}}"}
                )

                product_master_input_task = PythonOperator(
                    task_id = 'product_master_input_task',
                    python_callable = product_master_input_task_func,
                    op_kwargs = {"data_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_1.getting_product_data_1.web_data_get_task', key = 'data_list')}}"}
                )

                product_input_task = PythonOperator(
                    task_id = 'product_input_task',
                    python_callable = product_input_task_func,
                    op_kwargs = {"data_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_1.getting_product_data_1.web_data_get_task', key = 'data_list')}}"}
                )

                web_data_get_task >> product_master_input_task >> product_input_task

            with TaskGroup(group_id = 'getting_product_data_2', default_args = default_args) as getting_product_data_2:

                web_data_get_task = PythonOperator(
                    task_id = 'web_data_get_task',
                    python_callable = web_data_get_task_func,
                    op_kwargs = {"product_link_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_1.product_list_loader_task', key = 'dict_of_product_page')['part_2']}}"}
                )

                product_master_input_task = PythonOperator(
                    task_id = 'product_master_input_task',
                    python_callable = product_master_input_task_func,
                    op_kwargs = {"data_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_1.getting_product_data_2.web_data_get_task', key = 'data_list')}}"}
                )

                product_input_task = PythonOperator(
                    task_id = 'product_input_task',
                    python_callable = product_input_task_func,
                    op_kwargs = {"data_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_1.getting_product_data_2.web_data_get_task', key = 'data_list')}}"}
                )

                web_data_get_task >> product_master_input_task >> product_input_task

            with TaskGroup(group_id = 'getting_product_data_3', default_args = default_args) as getting_product_data_3:

                web_data_get_task = PythonOperator(
                    task_id = 'web_data_get_task',
                    python_callable = web_data_get_task_func,
                    op_kwargs = {"product_link_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_1.product_list_loader_task', key = 'dict_of_product_page')['part_3']}}"}
                )

                product_master_input_task = PythonOperator(
                    task_id = 'product_master_input_task',
                    python_callable = product_master_input_task_func,
                    op_kwargs = {"data_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_1.getting_product_data_3.web_data_get_task', key = 'data_list')}}"}
                )

                product_input_task = PythonOperator(
                    task_id = 'product_input_task',
                    python_callable = product_input_task_func,
                    op_kwargs = {"data_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_1.getting_product_data_3.web_data_get_task', key = 'data_list')}}"}
                )

                web_data_get_task >> product_master_input_task >> product_input_task

            with TaskGroup(group_id = 'getting_product_data_4', default_args = default_args) as getting_product_data_4:

                web_data_get_task = PythonOperator(
                    task_id = 'web_data_get_task',
                    python_callable = web_data_get_task_func,
                    op_kwargs = {"product_link_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_1.product_list_loader_task', key = 'dict_of_product_page')['part_4']}}"}
                )

                product_master_input_task = PythonOperator(
                    task_id = 'product_master_input_task',
                    python_callable = product_master_input_task_func,
                    op_kwargs = {"data_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_1.getting_product_data_4.web_data_get_task', key = 'data_list')}}"}
                )

                product_input_task = PythonOperator(
                    task_id = 'product_input_task',
                    python_callable = product_input_task_func,
                    op_kwargs = {"data_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_1.getting_product_data_4.web_data_get_task', key = 'data_list')}}"}
                )

                web_data_get_task >> product_master_input_task >> product_input_task

            with TaskGroup(group_id = 'getting_product_data_5', default_args = default_args) as getting_product_data_5:

                web_data_get_task = PythonOperator(
                    task_id = 'web_data_get_task',
                    python_callable = web_data_get_task_func,
                    op_kwargs = {"product_link_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_1.product_list_loader_task', key = 'dict_of_product_page')['part_5']}}"}
                )

                product_master_input_task = PythonOperator(
                    task_id = 'product_master_input_task',
                    python_callable = product_master_input_task_func,
                    op_kwargs = {"data_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_1.getting_product_data_5.web_data_get_task', key = 'data_list')}}"}
                )

                product_input_task = PythonOperator(
                    task_id = 'product_input_task',
                    python_callable = product_input_task_func,
                    op_kwargs = {"data_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_1.getting_product_data_5.web_data_get_task', key = 'data_list')}}"}
                )

                web_data_get_task >> product_master_input_task >> product_input_task

            with TaskGroup(group_id = 'getting_product_data_6', default_args = default_args) as getting_product_data_6:

                web_data_get_task = PythonOperator(
                    task_id = 'web_data_get_task',
                    python_callable = web_data_get_task_func,
                    op_kwargs = {"product_link_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_1.product_list_loader_task', key = 'dict_of_product_page')['part_6']}}"}
                )

                product_master_input_task = PythonOperator(
                    task_id = 'product_master_input_task',
                    python_callable = product_master_input_task_func,
                    op_kwargs = {"data_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_1.getting_product_data_6.web_data_get_task', key = 'data_list')}}"}
                )

                product_input_task = PythonOperator(
                    task_id = 'product_input_task',
                    python_callable = product_input_task_func,
                    op_kwargs = {"data_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_1.getting_product_data_6.web_data_get_task', key = 'data_list')}}"}
                )

                web_data_get_task >> product_master_input_task >> product_input_task

            with TaskGroup(group_id = 'getting_product_data_7', default_args = default_args) as getting_product_data_7:

                web_data_get_task = PythonOperator(
                    task_id = 'web_data_get_task',
                    python_callable = web_data_get_task_func,
                    op_kwargs = {"product_link_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_1.product_list_loader_task', key = 'dict_of_product_page')['part_7']}}"}
                )

                product_master_input_task = PythonOperator(
                    task_id = 'product_master_input_task',
                    python_callable = product_master_input_task_func,
                    op_kwargs = {"data_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_1.getting_product_data_7.web_data_get_task', key = 'data_list')}}"}
                )

                product_input_task = PythonOperator(
                    task_id = 'product_input_task',
                    python_callable = product_input_task_func,
                    op_kwargs = {"data_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_1.getting_product_data_7.web_data_get_task', key = 'data_list')}}"}
                )

                web_data_get_task >> product_master_input_task >> product_input_task

            with TaskGroup(group_id = 'getting_product_data_8', default_args = default_args) as getting_product_data_8:

                web_data_get_task = PythonOperator(
                    task_id = 'web_data_get_task',
                    python_callable = web_data_get_task_func,
                    op_kwargs = {"product_link_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_1.product_list_loader_task', key = 'dict_of_product_page')['part_8']}}"}
                )

                product_master_input_task = PythonOperator(
                    task_id = 'product_master_input_task',
                    python_callable = product_master_input_task_func,
                    op_kwargs = {"data_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_1.getting_product_data_8.web_data_get_task', key = 'data_list')}}"}
                )

                product_input_task = PythonOperator(
                    task_id = 'product_input_task',
                    python_callable = product_input_task_func,
                    op_kwargs = {"data_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_1.getting_product_data_8.web_data_get_task', key = 'data_list')}}"}
                )

                web_data_get_task >> product_master_input_task >> product_input_task

            with TaskGroup(group_id = 'getting_product_data_9', default_args = default_args) as getting_product_data_9:

                web_data_get_task = PythonOperator(
                    task_id = 'web_data_get_task',
                    python_callable = web_data_get_task_func,
                    op_kwargs = {"product_link_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_1.product_list_loader_task', key = 'dict_of_product_page')['part_9']}}"}
                )

                product_master_input_task = PythonOperator(
                    task_id = 'product_master_input_task',
                    python_callable = product_master_input_task_func,
                    op_kwargs = {"data_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_1.getting_product_data_9.web_data_get_task', key = 'data_list')}}"}
                )

                product_input_task = PythonOperator(
                    task_id = 'product_input_task',
                    python_callable = product_input_task_func,
                    op_kwargs = {"data_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_1.getting_product_data_9.web_data_get_task', key = 'data_list')}}"}
                )

                web_data_get_task >> product_master_input_task >> product_input_task

            with TaskGroup(group_id = 'getting_product_data_10', default_args = default_args) as getting_product_data_10:

                web_data_get_task = PythonOperator(
                    task_id = 'web_data_get_task',
                    python_callable = web_data_get_task_func,
                    op_kwargs = {"product_link_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_1.product_list_loader_task', key = 'dict_of_product_page')['part_10']}}"}
                )

                product_master_input_task = PythonOperator(
                    task_id = 'product_master_input_task',
                    python_callable = product_master_input_task_func,
                    op_kwargs = {"data_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_1.getting_product_data_10.web_data_get_task', key = 'data_list')}}"}
                )

                product_input_task = PythonOperator(
                    task_id = 'product_input_task',
                    python_callable = product_input_task_func,
                    op_kwargs = {"data_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_1.getting_product_data_10.web_data_get_task', key = 'data_list')}}"}
                )

                web_data_get_task >> product_master_input_task >> product_input_task

            product_list_loader_task >> [getting_product_data_1, getting_product_data_2, getting_product_data_3, getting_product_data_4, getting_product_data_5, getting_product_data_6, getting_product_data_7, getting_product_data_8, getting_product_data_9, getting_product_data_10]

        with TaskGroup(group_id = 'getting_product_link_2', default_args = default_args) as getting_product_link_2:

            product_list_loader_task = PythonOperator(
                task_id = 'product_list_loader_task',
                python_callable = product_list_loader_task_func,
                op_kwargs = {"page_url_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.shop_page_list_collect_task', key = 'dict_of_shop_page')['part_2']}}"}
            )

            with TaskGroup(group_id = 'getting_product_data_1', default_args = default_args) as getting_product_data_1:

                web_data_get_task = PythonOperator(
                    task_id = 'web_data_get_task',
                    python_callable = web_data_get_task_func,
                    op_kwargs = {"product_link_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_2.product_list_loader_task', key = 'dict_of_product_page')['part_1']}}"}
                )

                product_master_input_task = PythonOperator(
                    task_id = 'product_master_input_task',
                    python_callable = product_master_input_task_func,
                    op_kwargs = {"data_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_2.getting_product_data_1.web_data_get_task', key = 'data_list')}}"}
                )

                product_input_task = PythonOperator(
                    task_id = 'product_input_task',
                    python_callable = product_input_task_func,
                    op_kwargs = {"data_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_2.getting_product_data_1.web_data_get_task', key = 'data_list')}}"}
                )

                web_data_get_task >> product_master_input_task >> product_input_task

            with TaskGroup(group_id = 'getting_product_data_2', default_args = default_args) as getting_product_data_2:

                web_data_get_task = PythonOperator(
                    task_id = 'web_data_get_task',
                    python_callable = web_data_get_task_func,
                    op_kwargs = {"product_link_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_2.product_list_loader_task', key = 'dict_of_product_page')['part_2']}}"}
                )

                product_master_input_task = PythonOperator(
                    task_id = 'product_master_input_task',
                    python_callable = product_master_input_task_func,
                    op_kwargs = {"data_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_2.getting_product_data_2.web_data_get_task', key = 'data_list')}}"}
                )

                product_input_task = PythonOperator(
                    task_id = 'product_input_task',
                    python_callable = product_input_task_func,
                    op_kwargs = {"data_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_2.getting_product_data_2.web_data_get_task', key = 'data_list')}}"}
                )

                web_data_get_task >> product_master_input_task >> product_input_task

            with TaskGroup(group_id = 'getting_product_data_3', default_args = default_args) as getting_product_data_3:

                web_data_get_task = PythonOperator(
                    task_id = 'web_data_get_task',
                    python_callable = web_data_get_task_func,
                    op_kwargs = {"product_link_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_2.product_list_loader_task', key = 'dict_of_product_page')['part_3']}}"}
                )

                product_master_input_task = PythonOperator(
                    task_id = 'product_master_input_task',
                    python_callable = product_master_input_task_func,
                    op_kwargs = {"data_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_2.getting_product_data_3.web_data_get_task', key = 'data_list')}}"}
                )

                product_input_task = PythonOperator(
                    task_id = 'product_input_task',
                    python_callable = product_input_task_func,
                    op_kwargs = {"data_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_2.getting_product_data_3.web_data_get_task', key = 'data_list')}}"}
                )

                web_data_get_task >> product_master_input_task >> product_input_task

            with TaskGroup(group_id = 'getting_product_data_4', default_args = default_args) as getting_product_data_4:

                web_data_get_task = PythonOperator(
                    task_id = 'web_data_get_task',
                    python_callable = web_data_get_task_func,
                    op_kwargs = {"product_link_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_2.product_list_loader_task', key = 'dict_of_product_page')['part_4']}}"}
                )

                product_master_input_task = PythonOperator(
                    task_id = 'product_master_input_task',
                    python_callable = product_master_input_task_func,
                    op_kwargs = {"data_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_2.getting_product_data_4.web_data_get_task', key = 'data_list')}}"}
                )

                product_input_task = PythonOperator(
                    task_id = 'product_input_task',
                    python_callable = product_input_task_func,
                    op_kwargs = {"data_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_2.getting_product_data_4.web_data_get_task', key = 'data_list')}}"}
                )

                web_data_get_task >> product_master_input_task >> product_input_task

            with TaskGroup(group_id = 'getting_product_data_5', default_args = default_args) as getting_product_data_5:

                web_data_get_task = PythonOperator(
                    task_id = 'web_data_get_task',
                    python_callable = web_data_get_task_func,
                    op_kwargs = {"product_link_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_2.product_list_loader_task', key = 'dict_of_product_page')['part_5']}}"}
                )

                product_master_input_task = PythonOperator(
                    task_id = 'product_master_input_task',
                    python_callable = product_master_input_task_func,
                    op_kwargs = {"data_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_2.getting_product_data_5.web_data_get_task', key = 'data_list')}}"}
                )

                product_input_task = PythonOperator(
                    task_id = 'product_input_task',
                    python_callable = product_input_task_func,
                    op_kwargs = {"data_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_2.getting_product_data_5.web_data_get_task', key = 'data_list')}}"}
                )

                web_data_get_task >> product_master_input_task >> product_input_task

            with TaskGroup(group_id = 'getting_product_data_6', default_args = default_args) as getting_product_data_6:

                web_data_get_task = PythonOperator(
                    task_id = 'web_data_get_task',
                    python_callable = web_data_get_task_func,
                    op_kwargs = {"product_link_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_2.product_list_loader_task', key = 'dict_of_product_page')['part_6']}}"}
                )

                product_master_input_task = PythonOperator(
                    task_id = 'product_master_input_task',
                    python_callable = product_master_input_task_func,
                    op_kwargs = {"data_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_2.getting_product_data_6.web_data_get_task', key = 'data_list')}}"}
                )

                product_input_task = PythonOperator(
                    task_id = 'product_input_task',
                    python_callable = product_input_task_func,
                    op_kwargs = {"data_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_2.getting_product_data_6.web_data_get_task', key = 'data_list')}}"}
                )

                web_data_get_task >> product_master_input_task >> product_input_task

            with TaskGroup(group_id = 'getting_product_data_7', default_args = default_args) as getting_product_data_7:

                web_data_get_task = PythonOperator(
                    task_id = 'web_data_get_task',
                    python_callable = web_data_get_task_func,
                    op_kwargs = {"product_link_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_2.product_list_loader_task', key = 'dict_of_product_page')['part_7']}}"}
                )

                product_master_input_task = PythonOperator(
                    task_id = 'product_master_input_task',
                    python_callable = product_master_input_task_func,
                    op_kwargs = {"data_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_2.getting_product_data_7.web_data_get_task', key = 'data_list')}}"}
                )

                product_input_task = PythonOperator(
                    task_id = 'product_input_task',
                    python_callable = product_input_task_func,
                    op_kwargs = {"data_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_2.getting_product_data_7.web_data_get_task', key = 'data_list')}}"}
                )

                web_data_get_task >> product_master_input_task >> product_input_task

            with TaskGroup(group_id = 'getting_product_data_8', default_args = default_args) as getting_product_data_8:

                web_data_get_task = PythonOperator(
                    task_id = 'web_data_get_task',
                    python_callable = web_data_get_task_func,
                    op_kwargs = {"product_link_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_2.product_list_loader_task', key = 'dict_of_product_page')['part_8']}}"}
                )

                product_master_input_task = PythonOperator(
                    task_id = 'product_master_input_task',
                    python_callable = product_master_input_task_func,
                    op_kwargs = {"data_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_2.getting_product_data_8.web_data_get_task', key = 'data_list')}}"}
                )

                product_input_task = PythonOperator(
                    task_id = 'product_input_task',
                    python_callable = product_input_task_func,
                    op_kwargs = {"data_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_2.getting_product_data_8.web_data_get_task', key = 'data_list')}}"}
                )

                web_data_get_task >> product_master_input_task >> product_input_task

            with TaskGroup(group_id = 'getting_product_data_9', default_args = default_args) as getting_product_data_9:

                web_data_get_task = PythonOperator(
                    task_id = 'web_data_get_task',
                    python_callable = web_data_get_task_func,
                    op_kwargs = {"product_link_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_2.product_list_loader_task', key = 'dict_of_product_page')['part_9']}}"}
                )

                product_master_input_task = PythonOperator(
                    task_id = 'product_master_input_task',
                    python_callable = product_master_input_task_func,
                    op_kwargs = {"data_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_2.getting_product_data_9.web_data_get_task', key = 'data_list')}}"}
                )

                product_input_task = PythonOperator(
                    task_id = 'product_input_task',
                    python_callable = product_input_task_func,
                    op_kwargs = {"data_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_2.getting_product_data_9.web_data_get_task', key = 'data_list')}}"}
                )

                web_data_get_task >> product_master_input_task >> product_input_task

            with TaskGroup(group_id = 'getting_product_data_10', default_args = default_args) as getting_product_data_10:

                web_data_get_task = PythonOperator(
                    task_id = 'web_data_get_task',
                    python_callable = web_data_get_task_func,
                    op_kwargs = {"product_link_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_2.product_list_loader_task', key = 'dict_of_product_page')['part_10']}}"}
                )

                product_master_input_task = PythonOperator(
                    task_id = 'product_master_input_task',
                    python_callable = product_master_input_task_func,
                    op_kwargs = {"data_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_2.getting_product_data_10.web_data_get_task', key = 'data_list')}}"}
                )

                product_input_task = PythonOperator(
                    task_id = 'product_input_task',
                    python_callable = product_input_task_func,
                    op_kwargs = {"data_list": "{{task_instance.xcom_pull(task_ids = 'scrapping_process.getting_product_link_2.getting_product_data_10.web_data_get_task', key = 'data_list')}}"}
                )

                web_data_get_task >> product_master_input_task >> product_input_task

            product_list_loader_task >> [getting_product_data_1, getting_product_data_2, getting_product_data_3, getting_product_data_4, getting_product_data_5, getting_product_data_6, getting_product_data_7, getting_product_data_8, getting_product_data_9, getting_product_data_10]


        run_firefox_task >> shop_page_list_collect_task >> [getting_product_link_1, getting_product_link_2] 

    pricerecommendation_ml_task = BashOperator(
        task_id = 'pricerecommendation_ml_task',
        bash_command = 'python /opt/airflow/dags/tokopedia_unilever_scrapping_pipeline/pricerecommendation.py',
        dag = dag,
    )

    scrapping_process >> pricerecommendation_ml_task
