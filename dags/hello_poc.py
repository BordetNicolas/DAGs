from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import time


def hello():
    print("Hello depuis Python")
    time.sleep(10)


with DAG(
    dag_id="poc_simple",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    bash = BashOperator(
        task_id="bash_task",
        bash_command="echo 'Hello depuis Bash' && sleep 10"
    )

    python = PythonOperator(
        task_id="python_task",
        python_callable=hello
    )

    bash_2 = BashOperator(
        task_id="bash_task_2",
        bash_command="echo 'Hello depuis Bash 2' && sleep 10"
    )

    bash_3 = BashOperator(
        task_id="bash_task_3",
        bash_command="echo 'Hello depuis Bash 3' && sleep 10"
    )

    bash_4 = BashOperator(
        task_id="bash_task_4",
        bash_command="echo 'Hello depuis Bash 4' && sleep 10"
    )

    bash_5 = BashOperator(
        task_id="bash_task_5",
        bash_command="echo 'Hello depuis Bash 5' && sleep 10"
    )

    bash >> python >> [bash_2, bash_4, bash_5] >> bash_3
