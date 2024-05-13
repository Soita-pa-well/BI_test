from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from main import main

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 5),
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

institute_task = PythonOperator(
    task_id='institute_task',
    python_callable=main,
    dag=dag,
)
