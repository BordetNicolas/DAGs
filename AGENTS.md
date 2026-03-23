# AGENTS.md — Apache Airflow DAGs Repository

This file provides guidance for agentic coding agents working in this repository.

---

## Repository Overview

A pure **Apache Airflow 2.x** DAG collection written in Python. The repository contains
demonstration and stress-testing DAGs. There is no build system, no package manifest, and
no test suite — DAGs are validated by deploying them into a running Airflow environment.

```
DAGs/
├── AGENTS.md
├── README.md
└── dags/
    ├── hello_poc.py        # Simple retry/failure-simulation DAG
    ├── advanced_example.py # Branching, TaskGroups, TriggerRules
    ├── chaine_directe.py   # Sequential chain with parallel branches
    └── stress_test.py      # Progressive load-testing DAG
```

---

## Build / Lint / Test Commands

No build system or test framework is configured. All validation happens inside Airflow.

### Validate DAG syntax locally (no Airflow server required)

```bash
# Parse a single DAG file for import/syntax errors
python dags/hello_poc.py

# If Airflow CLI is available, list DAGs to confirm correct parsing
airflow dags list

# Test a single task without running the full DAG
airflow tasks test <dag_id> <task_id> <execution_date>
# Example:
airflow tasks test poc_simple bash_task 2024-01-01
```

### Trigger a full DAG run manually

```bash
airflow dags trigger <dag_id>
# Example:
airflow dags trigger stress_test_cluster
```

### Lint (manual, no config file enforced)

```bash
# Run flake8 over the dags/ directory (install separately if needed)
flake8 dags/

# Or with ruff (faster)
ruff check dags/
```

No `.flake8`, `pyproject.toml`, or `ruff.toml` config files exist yet. If you add linting
configuration, place it in a `pyproject.toml` at the repo root.

---

## Code Style Guidelines

### Language and Framework

- **Python 3.8+**, **Apache Airflow 2.x** only.
- All DAG logic lives in `dags/`. Never add subdirectories unless necessary.
- No custom classes — use functions and Airflow operators exclusively.

### Imports

Always order imports as follows (no blank line between groups 1–2; blank line before stdlib):

```python
# 1. Airflow framework imports first
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

# 2. Standard library (datetime always uses selective import)
from datetime import datetime, timedelta
import logging
import os
import time
```

- Use **absolute imports only** — no relative imports.
- Use `from datetime import datetime, timedelta` (selective), not `import datetime`.
- Use `import module` (plain) for all other stdlib modules (`logging`, `os`, `time`, etc.).
- No third-party libraries beyond Airflow.

### Formatting

- **4-space indentation** — no tabs.
- **Max line length: ~100 characters** (long strings may exceed this; wrap with parentheses).
- Wrap long `logger.info()` f-strings using implicit string concatenation inside parentheses:

```python
logger.info(
    f"Stress I/O démarré — fichier={file_size_mb} Mo, "
    f"itérations={iterations}"
)
```

- Multi-line `bash_command` strings: use a plain parenthesised string, not triple-quotes.

### Naming Conventions

| Element | Convention | Example |
|---|---|---|
| Files | `snake_case.py` | `chaine_directe.py` |
| Functions | `snake_case` | `stress_cpu`, `log_passthrough` |
| Variables / task refs | `snake_case` | `bash`, `checkpoint_1` |
| Constants (module-level) | `UPPER_SNAKE_CASE` | `RANDOM_FAIL_CMD` |
| DAG IDs | `snake_case` string | `"stress_test_cluster"` |
| Task IDs | `snake_case` string | `"bash_task"`, `"controle_qualite"` |
| TaskGroup IDs | `snake_case` string | `"vague_1_cpu"`, `"extraction"` |
| `op_kwargs` keys | `snake_case` string | `"task_name"`, `"duration_seconds"` |

### Type Annotations

- Annotate **function parameters** with types on all non-trivial callables.
- Do **not** annotate return types (project convention — `-> None` is omitted).
- Always include `**kwargs` on any function used as an Airflow callable, to receive context.
- Default values on parameters are preferred over hardcoding inside the function body.

```python
# Good
def stress_cpu(duration_seconds: int = 30, intensity: str = "medium", **kwargs):
    ...

# Bad — no types, no kwargs
def stress_cpu(duration, intensity):
    ...
```

### Docstrings

- All non-trivial functions get a one-line docstring.
- Include an `Args:` block when there are two or more parameters:

```python
def stress_io(file_size_mb: int = 100, iterations: int = 3, **kwargs):
    """Génère de la charge I/O via écriture/lecture de fichiers temporaires.

    Args:
        file_size_mb: taille du fichier temporaire (en Mo).
        iterations: nombre de cycles écriture/lecture/suppression.
    """
```

---

## Airflow DAG Patterns

### DAG Definition

Always use the `with DAG(...) as dag:` context manager. Required arguments:

```python
with DAG(
    dag_id="my_dag",          # snake_case, unique across the repo
    start_date=datetime(2024, 1, 1),
    schedule=None,            # manual trigger only — always set this
    catchup=False,            # always False
    tags=["tag1", "tag2"],    # optional but encouraged
) as dag:
```

### Task Dependencies

Use the `>>` bitshift operator. Fan-out with lists:

```python
start >> extraction >> quality_check
quality_check >> [transformation, alerte]
[transformation, alerte] >> load >> notifications >> end
```

### TaskGroups

Use `TaskGroup` for logical grouping of parallel tasks. IDs are `snake_case`:

```python
with TaskGroup("extraction") as extraction:
    extract_users = BashOperator(task_id="extract_users", ...)
    extract_orders = BashOperator(task_id="extract_orders", ...)
```

When a `BranchPythonOperator` targets a task inside a group, prefix the task ID:

```python
return "transformation.transform_a"  # group_id.task_id
```

### Retries

Always set `retries` explicitly on every operator. Use `retries=0` to mark tasks
that must not be retried (e.g., non-idempotent side-effecting tasks):

```python
PythonOperator(
    task_id="enrich",
    python_callable=random_fail,
    retries=3,
    retry_delay=timedelta(seconds=15),
)
```

### Logging

Use the Airflow task logger (not `print`) in structured callables:

```python
logger = logging.getLogger("airflow.task")
logger.info(f"Started — param={value}")
```

Use `print()` only in the simplest POC-style tasks.

### Topology Diagrams

Include an ASCII dependency diagram as a comment block above the dependencies section:

```python
# ══════════════════════════════════════════════
#  start
#    │
#    ▼
#  [extraction]  (3 tasks en parallèle)
#    │
#    ▼
#  controle_qualite
# ══════════════════════════════════════════════
```

---

## Error Handling

- **Do not use try/except** in Airflow callables — let failures bubble up to Airflow's
  retry and failure machinery.
- Simulate random failures with `raise Exception(...)` triggered by `random.random()`.
- Simulate Bash failures with shell exit codes: `exit 1` via `$((RANDOM % 5)) -lt 2`.
- Use `TriggerRule` to handle downstream tasks when upstream may be skipped/failed:

```python
EmptyOperator(
    task_id="end",
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
)
```

---

## Adding a New DAG

1. Create `dags/<name>.py` using `snake_case` for the filename.
2. Set `schedule=None` and `catchup=False` unless there is a specific reason not to.
3. Add `tags` for discoverability.
4. Include an ASCII diagram comment above the dependency block.
5. Validate locally: `python dags/<name>.py` must exit with no errors.
