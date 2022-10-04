from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd

dag = DAG(
    dag_id="etl-pipeline",
    description="Pipeline de extração de dados por OLTP",
    start_date=days_ago(2),
    schedule_interval="@daily",
)

def _extract():
    pass

def _transform():
    dados = pd.read_csv("/opt/airflow/scripts/data/estados_br.csv")
    dados_transformado = dados['Estados'].str.upper()
    dados_transformado.to_csv("/opt/airflow/scripts/data/estados_transformados.csv", index=False)

def _load():
    dados_final = pd.read_csv("/opt/airflow/scripts/data/estados_transformados.csv")
    print(dados_final.head(10))


extract_task =  BashOperator(
    task_id='Extract_Pandas_Data',
    bash_command='python /opt/airflow/scripts/model/extract.py',
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
