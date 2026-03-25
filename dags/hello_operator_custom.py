from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import logging
import os
import sys


logger = logging.getLogger("airflow.task")

CUSTOM_CODE_REPO_PATH = "/opt/airflow/custom_code/repo"

# Import du custom operator HelloOperator
# - En environnement Airflow, le dossier /opt/airflow/... doit exister.
# - En local (dans ce repo), on fait un fallback sur EmptyOperator uniquement pour
#   valider la syntaxe du DAG sans lever d'erreur d'import.
HelloOperator = EmptyOperator
if os.path.isdir(CUSTOM_CODE_REPO_PATH):
    if CUSTOM_CODE_REPO_PATH not in sys.path:
        sys.path.insert(0, CUSTOM_CODE_REPO_PATH)

    try:
        # Cas le plus fréquent: hello_operator.py expose HelloOperator
        from operators.hello_operator import HelloOperator  # type: ignore
    except Exception:
        # Alternative si HelloOperator est exporté directement depuis operators/
        from operators import HelloOperator  # type: ignore


# ══════════════════════════════════════════════
# DAG : HelloOperator — intégration custom
# ══════════════════════════════════════════════
#
#  start
#    │
#    ▼
#  hello_operator (custom)
#    │
#    ▼
#  end
#

with DAG(
    dag_id="hello_operator_custom",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # manuel uniquement
    catchup=False,
    tags=["custom", "operator", "hello"],
) as dag:

    start = EmptyOperator(
        task_id="start",
        retries=0,
    )

    hello_operator = HelloOperator(
        task_id="hello_operator",
        name="monde",
        retries=1,
        retry_delay=timedelta(seconds=10),
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_SUCCESS,
        retries=0,
    )

    start >> hello_operator >> end

