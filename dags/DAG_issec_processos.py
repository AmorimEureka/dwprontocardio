import os
from datetime import date, timedelta

import dlt
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from dlt.common import pendulum

from include.issec_portal import (
    DEFAULT_BASE_URL,
    portal_issec_source,
    select_months_to_scrape,
)


@dag(
    dag_id="extracao_processos_portal_issec",
    schedule="0 5 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Fortaleza"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
        "execution_timeout": timedelta(hours=4),
    },
    tags=["receita-certa", "issec", "scraping"],
)
def extracao_processos_portal_issec():
    @task
    def extrair_e_carregar():
        destination_conn_id = os.getenv(
            "ISSEC_DESTINATION_CONN_ID",
            "postgres_receita_certa",
        )
        pg_hook = PostgresHook(postgres_conn_id=destination_conn_id)
        pg_conn = pg_hook.get_connection(pg_hook.postgres_conn_id)
        os.environ["DESTINATION__CREDENTIALS"] = (
            f"postgresql://{pg_conn.login}:{pg_conn.password}"
            f"@{pg_conn.host}:{pg_conn.port}/{pg_conn.schema}"
        )

        extracted_months = set()
        try:
            rows = pg_hook.get_records(
                """
                SELECT DISTINCT DATE_TRUNC('month', mes_producao)::date
                  FROM raw_issec_portal.processos
                 WHERE mes_producao IS NOT NULL
                """
            )
            extracted_months = {row[0] for row in rows}
        except Exception:
            # Primeira execução: o dlt ainda criará schema e tabela.
            extracted_months = set()

        months = select_months_to_scrape(date.today(), extracted_months)
        if not months:
            return {"competencias": [], "registros": 0}

        source = portal_issec_source(
            username=os.environ["ISSEC_PORTAL_LOGIN"],
            password=os.environ["ISSEC_PORTAL_SENHA"],
            months=months,
            base_url=os.getenv("ISSEC_PORTAL_URL", DEFAULT_BASE_URL),
            timeout=int(os.getenv("ISSEC_PORTAL_TIMEOUT", "30")),
            max_workers=int(os.getenv("ISSEC_PORTAL_WORKERS", "4")),
        )

        pipeline = dlt.pipeline(
            pipeline_name="portal_issec_processos",
            dataset_name="raw_issec_portal",
            destination="postgres",
        )
        pipeline.run(source)
        return {
            "competencias": [month.strftime("%m/%Y") for month in months],
        }

    extrair_e_carregar()


dag = extracao_processos_portal_issec()
