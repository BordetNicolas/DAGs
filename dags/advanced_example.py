from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
import random


def choose_branch(**kwargs):
    """Simule un contrôle qualité : choisit aléatoirement si les données sont OK ou KO."""
    if random.choice([True, False]):
        print("Données OK -> on poursuit le traitement")
        return "transformation.transform_a"
    else:
        print("Données KO -> on envoie une alerte")
        return "alerte"


with DAG(
    dag_id="poc_avance",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["exemple", "avancé"],
) as dag:

    # ──────────────────────────────────────────────
    # ÉTAPE 1 : Démarrage
    # ──────────────────────────────────────────────
    start = EmptyOperator(task_id="start")

    # ──────────────────────────────────────────────
    # ÉTAPE 2 : Extraction en parallèle (TaskGroup)
    # ──────────────────────────────────────────────
    with TaskGroup("extraction") as extraction:
        extract_users = BashOperator(
            task_id="extract_users",
            bash_command="echo 'Extraction des utilisateurs' && sleep 3",
        )
        extract_orders = BashOperator(
            task_id="extract_orders",
            bash_command="echo 'Extraction des commandes' && sleep 5",
        )
        extract_products = BashOperator(
            task_id="extract_products",
            bash_command="echo 'Extraction des produits' && sleep 4",
        )

    # ──────────────────────────────────────────────
    # ÉTAPE 3 : Contrôle qualité (branchement)
    # ──────────────────────────────────────────────
    quality_check = BranchPythonOperator(
        task_id="controle_qualite",
        python_callable=choose_branch,
    )

    # ──────────────────────────────────────────────
    # ÉTAPE 4a : Transformation en parallèle (TaskGroup) — branche OK
    # ──────────────────────────────────────────────
    with TaskGroup("transformation") as transformation:
        transform_a = BashOperator(
            task_id="transform_a",
            bash_command="echo 'Transformation A : nettoyage' && sleep 3",
        )
        transform_b = BashOperator(
            task_id="transform_b",
            bash_command="echo 'Transformation B : enrichissement' && sleep 4",
        )
        transform_c = BashOperator(
            task_id="transform_c",
            bash_command="echo 'Transformation C : agrégation' && sleep 2",
        )

        # Dans le groupe : A lance B et C en parallèle
        transform_a >> [transform_b, transform_c]

    # ──────────────────────────────────────────────
    # ÉTAPE 4b : Alerte — branche KO
    # ──────────────────────────────────────────────
    alerte = BashOperator(
        task_id="alerte",
        bash_command="echo '⚠ ALERTE : données invalides, notification envoyée' && sleep 2",
    )

    # ──────────────────────────────────────────────
    # ÉTAPE 5 : Chargement (attend la fin d'une des deux branches)
    # ──────────────────────────────────────────────
    load = BashOperator(
        task_id="chargement",
        bash_command="echo 'Chargement des données en base' && sleep 3",
        # none_failed_min_one_success : se lance si au moins une branche
        # upstream a réussi et qu'aucune n'a échoué (les skipped sont ignorés)
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # ──────────────────────────────────────────────
    # ÉTAPE 6 : Notifications en parallèle
    # ──────────────────────────────────────────────
    with TaskGroup("notifications") as notifications:
        notify_email = BashOperator(
            task_id="notif_email",
            bash_command="echo 'Envoi notification email' && sleep 1",
        )
        notify_slack = BashOperator(
            task_id="notif_slack",
            bash_command="echo 'Envoi notification Slack' && sleep 1",
        )
        notify_log = BashOperator(
            task_id="notif_log",
            bash_command="echo 'Écriture dans les logs' && sleep 1",
        )

    # ──────────────────────────────────────────────
    # ÉTAPE 7 : Fin
    # ──────────────────────────────────────────────
    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # ══════════════════════════════════════════════
    # DÉPENDANCES
    # ══════════════════════════════════════════════
    #
    #  start
    #    │
    #    ▼
    #  [extraction]  (3 tasks en parallèle)
    #    │
    #    ▼
    #  controle_qualite
    #    ├──────────────────┐
    #    ▼                  ▼
    #  [transformation]   alerte
    #    │                  │
    #    └───────┬──────────┘
    #            ▼
    #       chargement
    #            │
    #            ▼
    #     [notifications]  (3 tasks en parallèle)
    #            │
    #            ▼
    #           end
    #

    start >> extraction >> quality_check
    quality_check >> transformation
    quality_check >> alerte
    [transformation, alerte] >> load >> notifications >> end
