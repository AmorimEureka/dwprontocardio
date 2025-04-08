
from pathlib import Path
from cosmos import ExecutionConfig

dbt_prontocardio_path = Path("/usr/local/airflow/dbt_prontocardio")
dbt_executable = Path("/usr/local/airflow/dbt_venv/bin/dbt")

venv_execution_config = ExecutionConfig(
    dbt_executable_path=str(dbt_executable),
)