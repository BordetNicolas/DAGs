from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time
import random


def hello():
    print("Hello depuis Python")
    time.sleep(3)
    # ~40% de chance d'échouer
    if random.random() < 0.4:
        raise Exception("Erreur aléatoire dans la task Python !")
    print("Python terminé avec succès")


# Commande bash qui échoue aléatoirement (~40% de chance)
# exit 1 = échec, exit 0 = succès
RANDOM_FAIL_CMD = (
    "echo 'Début de la task' && sleep 3 "
    "&& if [ $((RANDOM % 5)) -lt 2 ]; then echo 'ERREUR !' && exit 1; "
    "else echo 'Succès !'; fi"
)

with DAG(
    dag_id="poc_simple",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    # ── bash_task : PAS de retry ──
    # Si elle échoue, c'est fini → il faudra relancer manuellement
    bash = BashOperator(
        task_id="bash_task",
        bash_command=f"echo 'Hello depuis Bash' && {RANDOM_FAIL_CMD}",
        retries=0,
    )

    # ── python_task : AVEC retry (3 tentatives, 10s entre chaque) ──
    # Tu verras dans les logs les tentatives successives avant échec définitif
    python = PythonOperator(
        task_id="python_task",
        python_callable=hello,
        retries=3,
        retry_delay=timedelta(seconds=10),
    )

    # ── bash_task_2 : PAS de retry ──
    bash_2 = BashOperator(
        task_id="bash_task_2",
        bash_command=f"echo 'Hello depuis Bash 2' && {RANDOM_FAIL_CMD}",
        retries=0,
    )

    # ── bash_task_3 : AVEC retry (2 tentatives, 15s entre chaque) ──
    bash_3 = BashOperator(
        task_id="bash_task_3",
        bash_command=f"echo 'Hello depuis Bash 3' && {RANDOM_FAIL_CMD}",
        retries=2,
        retry_delay=timedelta(seconds=15),
    )

    # ── bash_task_4 : PAS de retry ──
    bash_4 = BashOperator(
        task_id="bash_task_4",
        bash_command=f"echo 'Hello depuis Bash 4' && {RANDOM_FAIL_CMD}",
        retries=0,
    )

    # ── bash_task_5 : AVEC retry (2 tentatives, 5s entre chaque) ──
    bash_5 = BashOperator(
        task_id="bash_task_5",
        bash_command=f"echo 'Hello depuis Bash 5' && {RANDOM_FAIL_CMD}",
        retries=2,
        retry_delay=timedelta(seconds=5),
    )

    bash >> python >> [bash_2, bash_4, bash_5] >> bash_3
