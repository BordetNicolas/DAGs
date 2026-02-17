from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import logging
import math
import os
import time


# ══════════════════════════════════════════════
# FONCTIONS DE CHARGE
# ══════════════════════════════════════════════

def stress_cpu(duration_seconds: int = 30, intensity: str = "medium", **kwargs):
    """Génère de la charge CPU via des calculs intensifs.

    Args:
        duration_seconds: durée du stress en secondes.
        intensity: "low", "medium" ou "high" — affecte la complexité des calculs.
    """
    logger = logging.getLogger("airflow.task")
    multiplier = {"low": 500, "medium": 5_000, "high": 50_000}.get(intensity, 5_000)
    logger.info(f"Stress CPU démarré — durée={duration_seconds}s, intensité={intensity}")

    end_time = time.time() + duration_seconds
    iterations = 0
    while time.time() < end_time:
        # Calculs flottants lourds
        for i in range(multiplier):
            _ = math.sqrt(i) * math.log(i + 1) * math.sin(i)
        iterations += 1

    logger.info(f"Stress CPU terminé — {iterations} itérations effectuées")


def stress_memory(target_mb: int = 256, hold_seconds: int = 20, **kwargs):
    """Alloue de la mémoire et la maintient pendant une durée donnée.

    Args:
        target_mb: quantité de mémoire à allouer (en Mo).
        hold_seconds: durée de rétention en secondes.
    """
    logger = logging.getLogger("airflow.task")
    logger.info(f"Stress mémoire démarré — cible={target_mb} Mo, maintien={hold_seconds}s")

    # Alloue par blocs de 1 Mo pour suivre la progression
    blocks = []
    for i in range(target_mb):
        blocks.append(bytearray(1024 * 1024))  # 1 Mo
        if (i + 1) % 50 == 0:
            logger.info(f"  Alloué {i + 1}/{target_mb} Mo")

    logger.info(f"  Allocation complète ({target_mb} Mo) — maintien {hold_seconds}s...")
    time.sleep(hold_seconds)

    # Libération explicite
    del blocks
    logger.info("Stress mémoire terminé — mémoire libérée")


def stress_io(file_size_mb: int = 100, iterations: int = 3, **kwargs):
    """Génère de la charge I/O via écriture/lecture de fichiers temporaires.

    Args:
        file_size_mb: taille du fichier temporaire (en Mo).
        iterations: nombre de cycles écriture/lecture/suppression.
    """
    logger = logging.getLogger("airflow.task")
    tmp_path = f"/tmp/airflow_stress_io_{os.getpid()}.bin"
    data = os.urandom(1024 * 1024)  # 1 Mo aléatoire

    logger.info(
        f"Stress I/O démarré — fichier={file_size_mb} Mo, "
        f"itérations={iterations}"
    )

    for cycle in range(iterations):
        # Écriture
        with open(tmp_path, "wb") as f:
            for _ in range(file_size_mb):
                f.write(data)
        logger.info(f"  Cycle {cycle + 1}/{iterations} — écriture OK ({file_size_mb} Mo)")

        # Lecture
        with open(tmp_path, "rb") as f:
            while f.read(1024 * 1024):
                pass
        logger.info(f"  Cycle {cycle + 1}/{iterations} — lecture OK")

        # Nettoyage
        os.remove(tmp_path)

    logger.info("Stress I/O terminé")


def stress_combined(cpu_seconds: int = 15, memory_mb: int = 128, **kwargs):
    """Charge combinée : alloue de la mémoire PUIS fait du calcul CPU dessus."""
    logger = logging.getLogger("airflow.task")
    logger.info(
        f"Stress combiné démarré — CPU={cpu_seconds}s, mémoire={memory_mb} Mo"
    )

    # Allocation mémoire
    blocks = [bytearray(1024 * 1024) for _ in range(memory_mb)]
    logger.info(f"  {memory_mb} Mo alloués — lancement du calcul CPU...")

    # Calcul CPU avec la mémoire allouée
    end_time = time.time() + cpu_seconds
    while time.time() < end_time:
        for block in blocks[:10]:
            for i in range(0, len(block), 4096):
                block[i] = (block[i] + 1) % 256

    del blocks
    logger.info("Stress combiné terminé")


def log_summary(phase: str, **kwargs):
    """Log un résumé de fin de phase."""
    logger = logging.getLogger("airflow.task")
    logger.info("━" * 60)
    logger.info(f"  Phase « {phase} » terminée avec succès")
    logger.info(f"  Execution date : {kwargs.get('execution_date', 'N/A')}")
    logger.info(f"  DAG run id     : {kwargs.get('run_id', 'N/A')}")
    logger.info("━" * 60)


# ══════════════════════════════════════════════
# DAG : Stress Test Cluster
# ══════════════════════════════════════════════
#
#  start
#    │
#    ▼
#  [vague_1_cpu]  (6 tasks CPU en parallèle)
#    │
#    ▼
#  checkpoint_1
#    │
#    ▼
#  [vague_2_mem_io]  (4 mémoire + 3 I/O en parallèle)
#    │
#    ▼
#  checkpoint_2
#    │
#    ▼
#  [vague_3_combinee]  (8 tasks combinées en parallèle)
#    │
#    ▼
#  checkpoint_3
#    │
#    ▼
#  [vague_4_burst]  (12 tasks légères en burst)
#    │
#    ▼
#  fin
#
# ══════════════════════════════════════════════

with DAG(
    dag_id="stress_test_cluster",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["stress", "montée en charge", "performance"],
    doc_md="""
## Stress Test Cluster

DAG conçu pour simuler une montée en charge progressive sur le cluster.

**4 vagues successives :**
1. **CPU** — 6 tâches de calcul intensif en parallèle
2. **Mémoire + I/O** — 4 tâches mémoire + 3 tâches I/O en parallèle
3. **Combinée** — 8 tâches CPU + mémoire simultanées
4. **Burst** — 12 tâches légères lancées d'un coup

Permet d'observer le comportement du scheduler, l'allocation des workers,
et la stabilité globale du cluster sous charge.
""",
) as dag:

    # ── Démarrage ──
    start = EmptyOperator(task_id="start")

    # ──────────────────────────────────────────────
    # VAGUE 1 : Stress CPU (6 tasks en parallèle)
    # ──────────────────────────────────────────────
    with TaskGroup("vague_1_cpu") as vague_1:
        for i in range(6):
            intensity = ["low", "medium", "high"][i % 3]
            PythonOperator(
                task_id=f"cpu_{intensity}_{i}",
                python_callable=stress_cpu,
                op_kwargs={
                    "duration_seconds": 20 + (i * 5),
                    "intensity": intensity,
                },
            )

    checkpoint_1 = PythonOperator(
        task_id="checkpoint_1",
        python_callable=log_summary,
        op_kwargs={"phase": "Vague 1 — CPU"},
    )

    # ──────────────────────────────────────────────
    # VAGUE 2 : Stress Mémoire + I/O (7 tasks en parallèle)
    # ──────────────────────────────────────────────
    with TaskGroup("vague_2_mem_io") as vague_2:
        for i in range(4):
            PythonOperator(
                task_id=f"memory_{i}",
                python_callable=stress_memory,
                op_kwargs={
                    "target_mb": 128 + (i * 64),
                    "hold_seconds": 15 + (i * 5),
                },
            )
        for i in range(3):
            PythonOperator(
                task_id=f"io_{i}",
                python_callable=stress_io,
                op_kwargs={
                    "file_size_mb": 50 + (i * 25),
                    "iterations": 2 + i,
                },
            )

    checkpoint_2 = PythonOperator(
        task_id="checkpoint_2",
        python_callable=log_summary,
        op_kwargs={"phase": "Vague 2 — Mémoire + I/O"},
    )

    # ──────────────────────────────────────────────
    # VAGUE 3 : Stress Combiné (8 tasks en parallèle)
    # ──────────────────────────────────────────────
    with TaskGroup("vague_3_combinee") as vague_3:
        for i in range(8):
            PythonOperator(
                task_id=f"combined_{i}",
                python_callable=stress_combined,
                op_kwargs={
                    "cpu_seconds": 10 + (i * 3),
                    "memory_mb": 64 + (i * 32),
                },
            )

    checkpoint_3 = PythonOperator(
        task_id="checkpoint_3",
        python_callable=log_summary,
        op_kwargs={"phase": "Vague 3 — Combinée"},
    )

    # ──────────────────────────────────────────────
    # VAGUE 4 : Burst (12 tasks légères simultanées)
    # Simule un pic de tâches pour stresser le scheduler
    # ──────────────────────────────────────────────
    with TaskGroup("vague_4_burst") as vague_4:
        for i in range(12):
            BashOperator(
                task_id=f"burst_{i}",
                bash_command=(
                    f"echo '[burst_{i}] Démarrage rapide' && "
                    f"sleep {3 + (i % 5)} && "
                    f"echo '[burst_{i}] Terminé'"
                ),
            )

    # ── Fin ──
    fin = EmptyOperator(
        task_id="fin",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ══════════════════════════════════════════════
    # DÉPENDANCES — montée en charge progressive
    # ══════════════════════════════════════════════
    start >> vague_1 >> checkpoint_1
    checkpoint_1 >> vague_2 >> checkpoint_2
    checkpoint_2 >> vague_3 >> checkpoint_3
    checkpoint_3 >> vague_4 >> fin
