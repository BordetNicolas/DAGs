from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import logging
import os
import sys


logger = logging.getLogger("airflow.task")

CUSTOM_CODE_REPO_PATHS = [
    "/opt/airflow/custom_code/repo",
    "/opt/airflow/custom_code",
    "/opt/airflow/plugins",
]

# Import du custom operator HelloOperator
# - En environnement Airflow, le dossier /opt/airflow/... doit exister.
# - En local (dans ce repo), on fait un fallback sur EmptyOperator uniquement pour
#   valider la syntaxe du DAG sans lever d'erreur d'import.
HelloOperator = EmptyOperator
_selected_repo_path = next((p for p in CUSTOM_CODE_REPO_PATHS if os.path.isdir(p)), None)
if _selected_repo_path:
    if _selected_repo_path not in sys.path:
        sys.path.insert(0, _selected_repo_path)

    try:
        # Cas le plus fréquent: operators/hello_operator.py expose HelloOperator
        from operators.hello_operator import HelloOperator  # type: ignore
        logger.info(f"HelloOperator importé depuis 'operators.hello_operator' via {_selected_repo_path}")
    except Exception as e:
        logger.info(
            "Import 'operators.hello_operator' impossible, tentative alternative "
            f"(repo_path={_selected_repo_path}, err={e!r})"
        )
        try:
            # Alternative si HelloOperator est exporté directement depuis operators/
            from operators import HelloOperator  # type: ignore
            logger.info(f"HelloOperator importé depuis 'operators' via {_selected_repo_path}")
        except Exception as e2:
            logger.info(
                "Import custom HelloOperator impossible, fallback sur EmptyOperator "
                f"(repo_path={_selected_repo_path}, err={e2!r})"
            )
else:
    logger.info(
        "Aucun repo custom trouvé (paths testés: "
        f"{', '.join(CUSTOM_CODE_REPO_PATHS)}). Fallback sur EmptyOperator."
    )


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
        retries=1,
        retry_delay=timedelta(seconds=10),
        **(
            {}
            if HelloOperator is EmptyOperator
            else {
                "name": "monde",
            }
        ),
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_SUCCESS,
        retries=0,
    )

    start >> hello_operator >> end

