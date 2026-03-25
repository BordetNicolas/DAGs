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

def _import_hello_operator():
    """Importe HelloOperator ou lève une erreur exploitable (pas de fallback)."""

    repo_paths = list(CUSTOM_CODE_REPO_PATHS)
    path_existence = {p: os.path.isdir(p) for p in repo_paths}
    import_errors = []

    for repo_path in repo_paths:
        if not path_existence[repo_path]:
            continue

        if repo_path not in sys.path:
            sys.path.insert(0, repo_path)

        try:
            # Cas le plus fréquent: operators/hello_operator.py expose HelloOperator
            from operators.hello_operator import HelloOperator  # type: ignore

            logger.info(f"HelloOperator importé depuis 'operators.hello_operator' via {repo_path}")
            return HelloOperator
        except Exception as e:
            import_errors.append(("operators.hello_operator", repo_path, repr(e)))

        try:
            # Alternative si HelloOperator est exporté directement depuis operators/
            from operators import HelloOperator  # type: ignore

            logger.info(f"HelloOperator importé depuis 'operators' via {repo_path}")
            return HelloOperator
        except Exception as e2:
            import_errors.append(("operators", repo_path, repr(e2)))

    details_lines = [
        "Impossible d'importer HelloOperator.",
        f"Paths testés: {repo_paths}",
        f"Présence des dossiers: {path_existence}",
    ]
    if import_errors:
        details_lines.append("Erreurs d'import rencontrées:")
        for module_name, repo_path, err in import_errors:
            details_lines.append(f"- module={module_name} repo_path={repo_path} err={err}")
    else:
        details_lines.append("Aucun des paths testés n'existe dans cet environnement.")

    raise ImportError("\n".join(details_lines))


HelloOperator = _import_hello_operator()


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

