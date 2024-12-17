from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from modules.etl import *

def fun_extract_top_countries(**kwargs):
    # pass
    extract_transform_top_countries()
    # uncomment function load and op load uncomment and use arm64 add into etl add extract_transform_top_countries, unpass 

def fun_load_top_countries(**kwargs):
    # pass
    load_top_countries()

def fun_extract_total_film(**kwargs):
    # pass
    extract_transform_total_film()
    # uncomment function load and op load uncomment and use arm64 add into etl add extract_transform_top_countries, unpass 

def fun_load_total_film(**kwargs):
    # pass
    load_total_film()

with DAG(
    dag_id='project3',
    start_date=datetime(2022, 5, 28),
    schedule_interval='00 23 * * *',
    catchup=False
) as dag:

    start_task = EmptyOperator(
        task_id='start'
    )

    op_extract_transform_top_countries = PythonOperator(
        task_id='extract_transform_top_countries',
        python_callable=fun_extract_top_countries
    )

    op_load_top_countries = PythonOperator(
        task_id='load_top_countries',
        python_callable=fun_load_top_countries
    )

    op_extract_transform_total_film = PythonOperator(
        task_id='extract_transform_total_film',
        python_callable=fun_extract_total_film
    )

    op_load_total_film = PythonOperator(
        task_id='load_total_film',
        python_callable=fun_load_total_film
    )
    end_task = EmptyOperator(
        task_id='end'
    )

start_task >> op_extract_transform_top_countries >> op_load_top_countries >> op_extract_transform_total_film >> op_load_total_film >> end_task