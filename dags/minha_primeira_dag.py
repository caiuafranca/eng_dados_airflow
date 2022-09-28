from airflow import DAG
import airflow.utils.dates
from airflow.operators.bash import BashOperator

dag = DAG(
    dag_id="hello_world",
    description = "A primeira DGA escrita em Python",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval="@daily",
)

task_echo_message = BashOperator (
    task_id="echo_message",
    bash_command=" echo  Hello World!",
    dag=dag,
)

task_echo_message