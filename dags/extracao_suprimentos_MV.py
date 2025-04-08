from airflow import DAG
from airflow.decorators import task
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from cosmos import DbtTaskGroup, ProjectConfig
from include.profiles import airflow_postgres_db
from include.constants import dbt_prontocardio_path, venv_execution_config

import os
import sqlalchemy as sa
from datetime import datetime, timedelta
import pandas as pd


postgres_hook = PostgresHook(postgres_conn_id='postgres_prontocardio')

engine_postgres = sa.create_engine(postgres_hook.get_uri())

nome_schema_postgres = 'raw_mv'

oracle_hook = OracleHook(oracle_conn_id='oracle_prontocardio', thick_mode=True)


def gerar_texto_querys(query: str, where: int):

    sql_dir = os.path.join(os.path.dirname(__file__), 'raw_mv', 'modelos')
    sql_path = os.path.join(sql_dir, f'{query}.sql')

    with open(sql_path, 'r') as arq:

        texto_query = arq.read()

        if ':PARAM' in texto_query:

            where = 1 if where == 0 else where

            texto_query = texto_query.replace(':PARAM', str(where))

        print(texto_query)
        return texto_query


def inserir_dados_postgres(tabela: str, colunas: str, place_insert: str, df: pd.DataFrame):

    print(colunas)
    print(place_insert)

    with postgres_hook.get_conn() as conn_pg:
        cursor_pg = conn_pg.cursor()

        query = f"INSERT INTO {nome_schema_postgres}.{tabela} ({colunas}) VALUES ({place_insert})"
        registros = [tuple(row) for row in df.to_numpy()]
        cursor_pg.executemany(query, registros)

        conn_pg.commit()


def apagar_tabela(tabela: str, conn):

    query = f"TRUNCATE TABLE {nome_schema_postgres}.{tabela} CASCADE"
    conn.execute(query)


default_args = {
    'owner': 'airflow_suprimentos',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id="Extracao_Suprimentos_MV",
    default_args=default_args,
    description="Extracao de Dados da Produção p/ Camada Raw - SUPRIMENTOS",
    schedule_interval=timedelta(minutes=20),
    catchup=False,
    max_active_runs=1,
) as dag:

    def extracao_oracle_for_postgres(nome_tabela, param):

        @task(task_id=f"obter_where_sql_{nome_tabela}")
        def obter_where_sql(nome_tabela: str):

            with postgres_hook.get_conn() as conn_pg:

                cursor_pg = conn_pg.cursor()

                if param in ['lista_tab_incremental', 'lista_tab_truncate']:

                    apagar_tabela(nome_tabela, cursor_pg) if nome_tabela in ['produto', 'est_pro'] else None

                    meta_campo = f"""
                                    SELECT UPPER(column_name)
                                    FROM information_schema.columns
                                    WHERE table_schema = '{nome_schema_postgres}'
                                    AND table_name = '{nome_tabela}'
                                    ORDER BY ordinal_position
                                    LIMIT 1 OFFSET 1;
                                """

                    cursor_pg.execute(meta_campo)
                    campo_result = cursor_pg.fetchone()
                    if campo_result is None:
                        return 0
                    campo = campo_result[0]
                    coluna = f'"{campo}"'
                    print(f"Coluna: {coluna}")

                    query_max_id = f"""
                                        SELECT {coluna}
                                        FROM {nome_schema_postgres}.{nome_tabela}
                                        WHERE id_{nome_tabela} = (
                                        SELECT MAX(id_{nome_tabela}) FROM {nome_schema_postgres}.{nome_tabela})
                                    """
                    cursor_pg.execute(query_max_id)
                    resultado = cursor_pg.fetchone()
                    where_sql = resultado[0] if resultado is not None else 0

                    cursor_pg.close()

                    return where_sql

                if param == 'lista_tab_snapshot':

                    tabela = nome_tabela

                    count_query = f"SELECT COUNT(id_{tabela}) FROM {nome_schema_postgres}.{tabela}"
                    cursor_pg.execute(count_query)
                    resultado = cursor_pg.fetchone()
                    resultado = resultado[0] if resultado is not None else 0

                    if resultado != 0:

                        if nome_tabela == 'ent_pro':
                            nome_tabela = 'entrada'

                        param_maior_dt_query = f"SELECT MAX(\"DT_{nome_tabela.upper()}\") FROM {nome_schema_postgres}.{tabela}"
                        cursor_pg.execute(param_maior_dt_query)
                        maior_dt_query = cursor_pg.fetchone()
                        where_sql = str(maior_dt_query[0]).split()[0] if maior_dt_query is not None else '2023-01-01'
                        apagar_tabela(tabela, cursor_pg)

                    else:

                        where_sql = '2023-01-01'

                    cursor_pg.close()

                    return where_sql



        @task(task_id=f"extrair_dados_oracle_{nome_tabela}")
        def extrair_dados_oracle(nome_tabela: str, maior_id: int):

            with oracle_hook.get_conn() as conn_ora:

                cursor_ora = conn_ora.cursor()
                texto_query = gerar_texto_querys(nome_tabela, maior_id)
                cursor_ora.execute(texto_query)

                metadados_query = cursor_ora.description
                colunas = [registro[0] for registro in metadados_query]
                nome_colunas = ', '.join(f'"{col}"'for col in colunas)
                placeholders_insert = ', '.join(['%s'] * len(colunas))

                cursor_ora.execute(texto_query)
                records = cursor_ora.fetchall()

                df = pd.DataFrame(records, columns=None)
                df = df.replace({pd.NaT: None})

                inserir_dados_postgres(nome_tabela, nome_colunas, placeholders_insert, df)

        where_sql_task = obter_where_sql(nome_tabela)
        extrair_dados_oracle_task = extrair_dados_oracle(nome_tabela, where_sql_task)
        where_sql_task >> extrair_dados_oracle_task
        return [where_sql_task, extrair_dados_oracle_task]

    lista_tabelas_postgres = {
        'lista_tab_incremental': [
            'uni_pro', 'mot_cancel', 'lot_pro', 'fornecedor', 'estoque',
            'setor', 'especie', 'mvto_estoque', 'itmvto_estoque', 'itsol_com',
            'itord_pro', 'itent_pro', 'atendime', 'convenio', 'gru_fat', 'gru_pro',
            'it_repasse_sih', 'it_repasse', 'paciente', 'prestador', 'pro_fat',
            'reg_amb', 'reg_fat', 'repasse', 'repasse_prestador', 'ati_med'
        ],
        'lista_tab_truncate': ['produto', 'est_pro'],
        'lista_tab_snapshot': ['sol_com', 'ord_com', 'ent_pro', 'itreg_amb', 'itreg_fat']
    }

    tarefas_extracao = []
    for tipo_param, lista in lista_tabelas_postgres.items():
        for nome_tabela in lista:
            tarefas_extracao.extend(extracao_oracle_for_postgres(nome_tabela, tipo_param))

    cosmos_dag_suprimentos = DbtTaskGroup(
        group_id="dbt_trf_suprimentos",
        project_config=ProjectConfig(dbt_prontocardio_path),
        profile_config=airflow_postgres_db,
        execution_config=venv_execution_config,
    )

    for tarefa in tarefas_extracao:
        tarefa >> cosmos_dag_suprimentos