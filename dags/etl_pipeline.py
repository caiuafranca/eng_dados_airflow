

from numpy import extract
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="etl-pipeline",
    description="Pipeline de extração de dados por OLTP",
    start_date=days_ago(2),
    schedule_interval="@daily",
)

def _extract():
    pass

def _transform():
    pass

def _load():
    pass


extract_task = PythonOperator(
    task_id="Extract_Dataset",
    python_callable=_extract,
    dag=dag
)

transform_task = PythonOperator(
    task_id="Transform_Dataset",
    python_callable=_transform,
    dag=dag
)

load_task = PythonOperator(
    task_id="Load_Dataset",
    python_callable=_load,
    dag=dag
)


extract_task >> transform_task >> load_task