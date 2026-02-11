

    Dag
        poc_simple

00:00:40.383

00:00:20.191

bash_task

python_task

wait_sftp_test_xml

cat_sftp_file
poc_simple
Schedule
Latest Run
2026-02-11 16:52:29
Next Run
Owner
airflow
Tags
Latest Dag Version
v3
Overview
Runs
Tasks
Calendar
Audit Log
Code
Details
Parsed at: 2026-02-11 21:40:12
Parse Duration: 00:00:00.060

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import time

def hello():
    print("Hello depuis Python 👋")
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

    bash >> python

