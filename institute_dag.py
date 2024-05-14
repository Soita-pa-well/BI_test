from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from tasks.connect_and_create_table import connect_and_create_table
from tasks.start_task import start
from tasks.finish_task import finish
from tasks.get_info_task import get_info
from tasks.transform_info import info_transformation
from tasks.load_info_task import load_info

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    dag_id='institute_dag',
    default_args=default_args,
    description='DAG для запуска скрипта',
    schedule_interval='0 3 * * *',
)

start_task = PythonOperator(
    task_id='start_task',
    python_callable=start,
    show_return_value_in_logs=True,
    dag=dag

)

create_db_task = PythonOperator(
    task_id='create_db_task',
    python_callable=connect_and_create_table,
    dag=dag
)

get_info_task = PythonOperator(
    task_id='get_info_task',
    python_callable=get_info,
    provide_context=True,
    dag=dag
)

transformation_info_task = PythonOperator(
    task_id='transformation_info_task',
    python_callable=info_transformation,
    provide_context=True,
    dag=dag
)


load_info_task = PythonOperator(
    task_id='load_info_task',
    python_callable=load_info,
    dag=dag
)


finish_task = PythonOperator(
    task_id='finish_task',
    python_callable=finish,
    show_return_value_in_logs=True,
    dag=dag)

start_task >> \
    create_db_task >> \
    get_info_task >> \
    transformation_info_task >> \
    load_info_task >> \
    finish_task
