import os
from datetime import timedelta

import dlt
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.oracle.hooks.oracle import OracleHook
# from airflow.models import Variable
from dlt.common import pendulum
from dlt.helpers.airflow_helper import PipelineTasksGroup

from cosmos import DbtTaskGroup, ProjectConfig
from include.profiles import perfil_postgres
from include.constants import path_prj_dbt, venv_execution_config, render_skip_tests_config


default_task_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": "test@test.com",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "execution_timeout": timedelta(hours=20),
}


@dag(
    dag_id="extracao_notas_fiscais",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_task_args,
)
def load_oracle_data():

# ============================================================================================
# #################################### CONEXAO POSTGRES ######################################
    pg_hook = PostgresHook(postgres_conn_id="postgres_prontocardio")
    conn = pg_hook.get_connection(pg_hook.postgres_conn_id)

    os.environ["DESTINATION__CREDENTIALS"] = (
        f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    )
# ============================================================================================

# ============================================================================================
# ##################################### CONEXAO ORACLE #######################################
    oracle_hook = OracleHook(oracle_conn_id='oracle_prontocardio', thick_mode=True)
    oracle_conn = oracle_hook.get_connection(oracle_hook.oracle_conn_id)

    os.environ["ORACLE_USER"] = oracle_conn.login
    os.environ["ORACLE_PASSWORD"] = oracle_conn.password
    os.environ["ORACLE_HOST"] = oracle_conn.host
    os.environ["ORACLE_PORT"] = str(oracle_conn.port)

    # Obtendo SERVICENAME do campo EXTRA como json
    import json
    extra = {}
    if oracle_conn.extra:
        try:
            extra = json.loads(oracle_conn.extra) if isinstance(oracle_conn.extra, str) else oracle_conn.extra
        except:
            extra = oracle_conn.extra_dejson if hasattr(oracle_conn, 'extra_dejson') else {}

    # Buscar service_name com fallback
    service_name = extra.get('service_name')
    os.environ["ORACLE_SERVICE"] = service_name

    # Desabilita partial parse do dbt para garantir recompilação completa
    os.environ["DBT_PARTIAL_PARSE"] = "0"

# ============================================================================================


    # set `use_data_folder` to True to store temporary data on the `data` bucket.
    # Use only when it does not fit on the local storage
    tasks_dlt = PipelineTasksGroup(
        "notas_fiscais_pipeline_decomposed", use_data_folder=False, wipe_local_data=True
    )

    # Importando script do pipeline
    from script_ingestao.source_ingestao import source_acumula_resources

    # Definindo pipeline dlt
    pipeline = dlt.pipeline(
        pipeline_name="entradas_notas_fiscais",
        dataset_name="raw_entradas_mv",
        destination="postgres",
        full_refresh=False,  # must be false if we decompose
        refresh="drop_sources",
    )

    # get the source with date logic from source_acumula_resources
    source = source_acumula_resources(tag="notas")

    # decompose="serialize" ira converter os resources do dlt
    # em tarefas do Airflow. Para desabilitar isso defina-o como "none"
    tsk_dlt = tasks_dlt.add_run(
        pipeline,
        source,
        decompose="serialize",
        trigger_rule="all_done",
        retries=0,
    )

    tsk_dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command="/usr/local/airflow/dbt_venv/bin/dbt deps --project-dir /usr/local/airflow/dbt_dw_entradas",
    )

    tsk_dbt = DbtTaskGroup(
        group_id="tasks_dbt_entradas",
        project_config=ProjectConfig(path_prj_dbt),
        profile_config=perfil_postgres,
        execution_config=venv_execution_config,
        render_config=render_skip_tests_config,
    )

    tsk_dlt >> tsk_dbt_deps >> tsk_dbt


dag = load_oracle_data()
