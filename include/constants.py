
from pathlib import Path
from cosmos import ExecutionConfig
from cosmos.config import RenderConfig

dbt_prontocardio_path = Path("/usr/local/airflow/dbt_prontocardio")
dbt_executable = Path("/usr/local/airflow/dbt_venv/bin/dbt")

venv_execution_config = ExecutionConfig(
    dbt_executable_path=str(dbt_executable),
)


path_prj_dbt = Path("/usr/local/airflow/dbt_dw_entradas")
venv_prj_dbt = Path("/usr/local/airflow/dbt_venv/bin/dbt")

venv_execution_config = ExecutionConfig(
    dbt_executable_path=str(venv_prj_dbt),
)

# Configuração de renderização para pular testes
render_skip_tests_config = RenderConfig(
    test_behavior="skip",
)