from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from main import main

dag = DAG(
    'my_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval='0 3 * * *'
)

institute_task = PythonOperator(
    task_id='institute_task',
    python_callable=main,
    dag=dag
)
