from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import random
import time


# ──────────────────────────────────────────────
# Fonctions Python
# ──────────────────────────────────────────────

def random_fail(task_name: str, fail_rate: float = 0.4, **kwargs):
    """Simule un traitement avec une chance d'échec aléatoire."""
    print(f"[{task_name}] Début du traitement...")
    time.sleep(2)
    if random.random() < fail_rate:
        raise Exception(f"[{task_name}] Erreur aléatoire !")
    print(f"[{task_name}] Traitement terminé avec succès")


def log_passthrough(task_name: str, **kwargs):
    """Opérateur qui log simplement le passage dans la tâche."""
    logger = logging.getLogger("airflow.task")
    logger.info("━" * 50)
    logger.info(f"  Passage dans la tâche : {task_name}")
    logger.info(f"  Execution date : {kwargs.get('execution_date', 'N/A')}")
    logger.info(f"  DAG run id     : {kwargs.get('run_id', 'N/A')}")
    logger.info("━" * 50)


# ──────────────────────────────────────────────
# Commande bash avec échec aléatoire (~40%)
# ──────────────────────────────────────────────
RANDOM_FAIL_CMD = (
    "sleep 2 "
    "&& if [ $((RANDOM % 5)) -lt 2 ]; then echo 'ERREUR !' && exit 1; "
    "else echo 'Succès !'; fi"
)


# ══════════════════════════════════════════════
# DAG : Chaîne Directe
# ══════════════════════════════════════════════
#
#  Compo
#    │
#    ▼
#  Regroup
#    ├────────────────┬──────────┐
#    ▼                ▼          ▼
#  Enrich → Imp     Arch       eGov
#
# ══════════════════════════════════════════════

with DAG(
    dag_id="chaine_directe",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["chaîne", "directe"],
) as dag:

    # ── Compo : avec retry ──
    compo = PythonOperator(
        task_id="compo",
        python_callable=random_fail,
        op_kwargs={"task_name": "Compo"},
        retries=2,
        retry_delay=timedelta(seconds=10),
    )

    # ── Regroup : log simple, pas de retry ──
    regroup = PythonOperator(
        task_id="regroup",
        python_callable=log_passthrough,
        op_kwargs={"task_name": "Regroup"},
        retries=0,
    )

    # ── Enrich : avec retry ──
    enrich = PythonOperator(
        task_id="enrich",
        python_callable=random_fail,
        op_kwargs={"task_name": "Enrich"},
        retries=3,
        retry_delay=timedelta(seconds=15),
    )

    # ── Imp : pas de retry ──
    imp = BashOperator(
        task_id="imp",
        bash_command=f"echo '[Imp] Début import' && {RANDOM_FAIL_CMD}",
        retries=0,
    )

    # ── Arch : avec retry ──
    arch = BashOperator(
        task_id="arch",
        bash_command=f"echo '[Arch] Début archivage' && {RANDOM_FAIL_CMD}",
        retries=2,
        retry_delay=timedelta(seconds=10),
    )

    # ── eGov : pas de retry ──
    egov = BashOperator(
        task_id="egov",
        bash_command=f"echo '[eGov] Début envoi eGov' && {RANDOM_FAIL_CMD}",
        retries=0,
    )

    # ══════════════════════════════════════════════
    # DÉPENDANCES
    # ══════════════════════════════════════════════
    compo >> regroup >> [enrich, arch, egov]
    enrich >> imp
