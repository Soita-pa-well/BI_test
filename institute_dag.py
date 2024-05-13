from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from main import main
from tasks.connect_and_create_table import connect_and_create_table
from tasks.start_task import start
from tasks.finish_task import finish
from tasks.get_info_task import get_info

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
    task_id='get_info_task ',
    python_callable=get_info,
    dag=dag
)

institute_task = PythonOperator(
    task_id='institute_task',
    python_callable=main,
    dag=dag
)

finish_task = PythonOperator(
    task_id='finish_task',
    python_callable=finish,
    show_return_value_in_logs=True,
    dag=dag)


start_task >> create_db_task >> get_info_task >> institute_task >> finish_task
