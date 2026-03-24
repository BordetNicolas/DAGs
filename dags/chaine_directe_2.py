from airflow import DAG
from airflow.decorators import task
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


# ──────────────────────────────────────────────
# Tâches TaskFlow (@task)
# ──────────────────────────────────────────────

@task
def regroup_and_sample(**kwargs):
    """Log le passage dans regroup et tire aléatoirement le nombre
    d'exécutions pour chaque branche (enrich/imp, arch, egov).

    Retourne trois listes de dicts utilisées par expand_kwargs() :
        enrich_kwargs  → N dicts {"index": i}  avec N ∈ [1, 10]
        arch_kwargs    → M dicts {"index": i}  avec M ∈ [1, 10]
        egov_kwargs    → P dicts {"index": i}  avec P ∈ [1, 10]
    N, M, P sont tirés indépendamment.
    """
    logger = logging.getLogger("airflow.task")
    logger.info("━" * 50)
    logger.info("  Passage dans la tâche : Regroup")
    logger.info(f"  Execution date : {kwargs.get('execution_date', 'N/A')}")
    logger.info(f"  DAG run id     : {kwargs.get('run_id', 'N/A')}")

    n_enrich = random.randint(1, 10)
    n_arch = random.randint(1, 10)
    n_egov = random.randint(1, 10)

    logger.info("  Nombre d'exécutions tirés au sort :")
    logger.info(f"    enrich + imp : {n_enrich}")
    logger.info(f"    arch         : {n_arch}")
    logger.info(f"    egov         : {n_egov}")
    logger.info("━" * 50)

    return {
        "enrich_kwargs": [{"index": i} for i in range(n_enrich)],
        "arch_kwargs":   [{"index": i} for i in range(n_arch)],
        "egov_kwargs":   [{"index": i} for i in range(n_egov)],
    }


@task
def extract_enrich_kwargs(counts: dict, **kwargs):
    """Extrait la liste de kwargs pour le mapping de enrich."""
    return counts["enrich_kwargs"]


@task
def extract_arch_kwargs(counts: dict, **kwargs):
    """Extrait la liste de kwargs pour le mapping de arch."""
    return counts["arch_kwargs"]


@task
def extract_egov_kwargs(counts: dict, **kwargs):
    """Extrait la liste de kwargs pour le mapping de egov."""
    return counts["egov_kwargs"]


@task(retries=3, retry_delay=timedelta(seconds=15))
def enrich(index: int):
    """Exécute une instance de enrich.

    Args:
        index: indice de l'instance (0-based).
    """
    logger = logging.getLogger("airflow.task")
    logger.info(f"[enrich_{index}] Début du traitement...")
    time.sleep(2)
    if random.random() < 0.4:
        raise Exception(f"[enrich_{index}] Erreur aléatoire !")
    logger.info(f"[enrich_{index}] Traitement terminé avec succès")
    # Retourne l'indice pour que imp_paired reçoive la bonne valeur
    return index


@task(retries=2)
def imp_paired(index: int):
    """Exécute une instance de imp, paire avec l'enrich_i correspondant.

    Args:
        index: indice transmis par enrich_i.
    """
    logger = logging.getLogger("airflow.task")
    logger.info(f"[imp_{index}] Début de l'import (suite de enrich_{index})...")
    time.sleep(2)
    logger.info(f"[imp_{index}] Import terminé avec succès")


@task(retries=2, retry_delay=timedelta(seconds=10))
def arch(index: int):
    """Exécute une instance de arch.

    Args:
        index: indice de l'instance (0-based).
    """
    logger = logging.getLogger("airflow.task")
    logger.info(f"[arch_{index}] Début de l'archivage...")
    time.sleep(2)
    if random.random() < 0.4:
        raise Exception(f"[arch_{index}] Erreur aléatoire !")
    logger.info(f"[arch_{index}] Archivage terminé avec succès")


@task(retries=2)
def egov(index: int):
    """Exécute une instance de egov.

    Args:
        index: indice de l'instance (0-based).
    """
    logger = logging.getLogger("airflow.task")
    logger.info(f"[egov_{index}] Début de l'envoi eGov...")
    time.sleep(2)
    if random.random() < 0.4:
        raise Exception(f"[egov_{index}] Erreur aléatoire !")
    logger.info(f"[egov_{index}] Envoi eGov terminé avec succès")


# ══════════════════════════════════════════════
# DAG : Chaîne Directe 2 — exécutions dynamiques
# ══════════════════════════════════════════════
#
#  compo
#    │
#    ▼
#  regroup  (détermine N, M, P ∈ [1, 10] indépendamment)
#    ├──────────────────────────┬──────────────────────────┐
#    ▼                          ▼                          ▼
#  enrich_0…enrich_N-1      arch_0…arch_M-1          egov_0…egov_P-1
#    │ (paire par paire)
#    ▼
#  imp_0…imp_N-1
#
# ══════════════════════════════════════════════

with DAG(
    dag_id="chaine_directe_2",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["chaîne", "directe", "dynamique"],
) as dag:

    # ── Compo : avec retry ──
    compo = PythonOperator(
        task_id="compo",
        python_callable=random_fail,
        op_kwargs={"task_name": "Compo"},
        retries=2,
        retry_delay=timedelta(seconds=10),
    )

    # ── Regroup : détermine les counts et retourne les listes de kwargs ──
    counts = regroup_and_sample()

    # ── Prépare les listes dynamiques via retour standard TaskFlow ──
    enrich_kwargs_list = extract_enrich_kwargs(counts)
    arch_kwargs_list = extract_arch_kwargs(counts)
    egov_kwargs_list = extract_egov_kwargs(counts)

    # ── enrich : N instances dynamiques (N déterminé par regroup) ──
    enrich_results = enrich.expand_kwargs(enrich_kwargs_list)

    # ── imp : N instances, paire par paire avec enrich (via valeur retournée) ──
    # enrich retourne son index → imp_paired reçoit chaque index dans le même ordre
    imp_paired.expand(index=enrich_results)

    # ── arch : M instances dynamiques ──
    arch.expand_kwargs(arch_kwargs_list)

    # ── egov : P instances dynamiques ──
    egov.expand_kwargs(egov_kwargs_list)

    # ══════════════════════════════════════════════
    # DÉPENDANCES
    # ══════════════════════════════════════════════
    compo >> counts
