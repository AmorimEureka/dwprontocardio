from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from cosmos import DbtTaskGroup, ProjectConfig
from include.profiles import airflow_postgres_db
from include.constants import suprimentos_path, venv_execution_config

import os
import sqlalchemy as sa
from datetime import datetime, timedelta
import pandas as pd


postgres_hook = PostgresHook(postgres_conn_id='postgres_prontocardio')

engine_postgres = sa.create_engine(postgres_hook.get_uri())

nome_schema_postgres = 'raw_mv'

oracle_hook = OracleHook(oracle_conn_id='oracle_prontocardio', thick_mode=True)



def criar_tabela_raw(tabela: str, conn):

    metadata = sa.MetaData(schema=nome_schema_postgres)

    id = f"id_{tabela}"

    if tabela == lista_tabelas_postgres[0]:     # uni_pro
        id_seq = sa.Sequence(f'id_{tabela}_seq', start=1, increment=1)
        nova_tabela = sa.Table(tabela, metadata,
                               sa.Column(id, sa.BigInteger, id_seq, server_default=id_seq.next_value(), primary_key=True),
                               sa.Column('CD_UNI_PRO', sa.String()),
                               sa.Column('CD_UNIDADE', sa.String()),
                               sa.Column('CD_PRODUTO', sa.String()),
                               sa.Column('VL_FATOR', sa.String()),
                               sa.Column('DS_UNIDADE', sa.String()),
                               sa.Column('TP_RELATORIOS', sa.String()),
                               sa.Column('SN_ATIVO', sa.String()),
                               sa.Column('DT_EXTRACAO', sa.DateTime, server_default=sa.text("NOW()")),
                               )
    
    if tabela == lista_tabelas_postgres[1]:     # mot_cancel
        id_seq = sa.Sequence(f'id_{tabela}_seq', start=1, increment=1)
        nova_tabela = sa.Table(tabela, metadata,
                               sa.Column(id, sa.BigInteger, id_seq, server_default=id_seq.next_value(), primary_key=True),
                                sa.Column('CD_MOT_CANCEL', sa.String()),
                                sa.Column('DS_MOT_CANCEL', sa.String()),
                                sa.Column('TP_MOT_CANCEL', sa.String()),
                                sa.Column('DT_EXTRACAO', sa.DateTime, server_default=sa.text("NOW()")),
                                )

    if tabela == lista_tabelas_postgres[2]:     # lot_pro
        id_seq = sa.Sequence(f'id_{tabela}_seq', start=1, increment=1)
        nova_tabela = sa.Table(tabela, metadata,
                               sa.Column(id, sa.BigInteger, id_seq, server_default=id_seq.next_value(), primary_key=True),
                                sa.Column('CD_LOT_PRO', sa.String()),
                                sa.Column('CD_ESTOQUE', sa.String()),
                                sa.Column('CD_PRODUTO', sa.String()),
                                sa.Column('CD_LOTE', sa.String()),
                                sa.Column('DT_VALIDADE', sa.DateTime),
                                sa.Column('QT_ESTOQUE_ATUAL', sa.String()),
                                sa.Column('DT_EXTRACAO', sa.DateTime, server_default=sa.text("NOW()")),
                                )
        
    if tabela == lista_tabelas_postgres[3]:     # fornecedor
        id_seq = sa.Sequence(f'id_{tabela}_seq', start=1, increment=1)
        nova_tabela = sa.Table(tabela, metadata,
                               sa.Column(id, sa.BigInteger, id_seq, server_default=id_seq.next_value(), primary_key=True),
                                sa.Column('CD_FORNECEDOR', sa.String()),
                                sa.Column('NM_FORNECEDOR', sa.String(255)),
                                sa.Column('NM_FANTASIA', sa.String(255)),
                                sa.Column('DT_INCLUSAO', sa.DateTime),
                                sa.Column('NR_CGC_CPF', sa.String()),
                                sa.Column('TP_FORNECEDOR', sa.String()),
                                sa.Column('DT_EXTRACAO', sa.DateTime, server_default=sa.text("NOW()")),
                                )

    if tabela == lista_tabelas_postgres[4]:     # estoque
        id_seq = sa.Sequence(f'id_{tabela}_seq', start=1, increment=1)
        nova_tabela = sa.Table(tabela, metadata,
                               sa.Column(id, sa.BigInteger, id_seq, server_default=id_seq.next_value(), primary_key=True),
                                sa.Column('CD_ESTOQUE', sa.String()),
                                sa.Column('CD_SETOR', sa.String()),
                                sa.Column('DS_ESTOQUE', sa.String()),
                                sa.Column('TP_ESTOQUE', sa.String()),
                                sa.Column('DT_EXTRACAO', sa.DateTime, server_default=sa.text("NOW()")),
                                )
        
    if tabela == lista_tabelas_postgres[5]:     # setor
        id_seq = sa.Sequence(f'id_{tabela}_seq', start=1, increment=1)
        nova_tabela = sa.Table(tabela, metadata,
                               sa.Column(id, sa.BigInteger, id_seq, server_default=id_seq.next_value(), primary_key=True),
                                sa.Column('CD_SETOR', sa.String()),
                                sa.Column('CD_FATOR', sa.String()),
                                sa.Column('CD_GRUPO_DE_CUSTO', sa.String()),
                                sa.Column('CD_SETOR_CUSTO', sa.String()),
                                sa.Column('NM_SETOR', sa.String()),
                                sa.Column('SN_ATIVO', sa.String()),
                                sa.Column('DT_EXTRACAO', sa.DateTime, server_default=sa.text("NOW()")),
                                )

    if tabela == lista_tabelas_postgres[6]:     # especie
        id_seq = sa.Sequence(f'id_{tabela}_seq', start=1, increment=1)
        nova_tabela = sa.Table(tabela, metadata,
                               sa.Column(id, sa.BigInteger, id_seq, server_default=id_seq.next_value(), primary_key=True),
                                sa.Column('CD_ESPECIE', sa.String()),
                                sa.Column('CD_ITEM_RES', sa.String()),
                                sa.Column('DS_ESPECIE', sa.String()),
                                sa.Column('DT_EXTRACAO', sa.DateTime, server_default=sa.text("NOW()")),
                                )

    if tabela == lista_tabelas_postgres[7]:     # sol_com
        id_seq = sa.Sequence(f'id_{tabela}_seq', start=1, increment=1)
        nova_tabela = sa.Table(tabela, metadata,
                               sa.Column(id, sa.BigInteger, id_seq, server_default=id_seq.next_value(), primary_key=True),
                                sa.Column('CD_SOL_COM', sa.String()),
                                sa.Column('CD_MOT_PED', sa.String()),
                                sa.Column('CD_SETOR', sa.String()),
                                sa.Column('CD_ESTOQUE', sa.String()),
                                sa.Column('CD_MOT_CANCEL', sa.String()),
                                sa.Column('CD_ATENDIME', sa.String()),
                                sa.Column('CD_USUARIO', sa.String()),
                                sa.Column('NM_SOLICITANTE', sa.String()),
                                sa.Column('DT_SOL_COM', sa.DateTime),
                                sa.Column('DT_CANCELAMENTO', sa.DateTime),
                                sa.Column('VL_TOTAL', sa.String()),
                                sa.Column('TP_SITUACAO', sa.String()),
                                sa.Column('TP_SOL_COM', sa.String()),
                                sa.Column('SN_URGENTE', sa.String()),
                                sa.Column('SN_APROVADA', sa.String()),
                                sa.Column('SN_OPME', sa.String()),
                                sa.Column('DT_EXTRACAO', sa.DateTime, server_default=sa.text("NOW()")),
                                )

    if tabela == lista_tabelas_postgres[8]:     # ord_com
        id_seq = sa.Sequence(f'id_{tabela}_seq', start=1, increment=1)
        nova_tabela = sa.Table(tabela, metadata,
                               sa.Column(id, sa.BigInteger, id_seq, server_default=id_seq.next_value(), primary_key=True),
                                sa.Column('CD_ORD_COM', sa.String()),
                                sa.Column('CD_ESTOQUE', sa.String()),
                                sa.Column('CD_FORNECEDOR', sa.String()),
                                sa.Column('CD_SOL_COM', sa.String()),
                                sa.Column('CD_MOT_CANCEL', sa.String()),
                                sa.Column('CD_USUARIO_CRIADOR_OC', sa.String()),
                                sa.Column('CD_ULTIMO_USU_ALT_OC', sa.String()),
                                sa.Column('DT_ORD_COM', sa.DateTime),
                                sa.Column('DT_CANCELAMENTO', sa.DateTime),
                                sa.Column('DT_AUTORIZACAO', sa.DateTime),
                                sa.Column('DT_ULTIMA_ALTERACAO_OC', sa.DateTime),
                                sa.Column('TP_SITUACAO', sa.String()),
                                sa.Column('TP_ORD_COM', sa.String()),
                                sa.Column('SN_AUTORIZADO', sa.String()),
                                sa.Column('DT_EXTRACAO', sa.DateTime, server_default=sa.text("NOW()")),
                                )
        
    if tabela == lista_tabelas_postgres[9]:     # ent_pro
        id_seq = sa.Sequence(f'id_{tabela}_seq', start=1, increment=1)
        nova_tabela = sa.Table(tabela, metadata,
                               sa.Column(id, sa.BigInteger, id_seq, server_default=id_seq.next_value(), primary_key=True),
                                sa.Column('CD_ENT_PRO', sa.String()),
                                sa.Column('CD_TIP_ENT', sa.String()),
                                sa.Column('CD_ESTOQUE', sa.String()),
                                sa.Column('CD_FORNECEDOR', sa.String()),
                                sa.Column('CD_ORD_COM', sa.String()),
                                sa.Column('CD_USUARIO_RECEBIMENTO', sa.String()),
                                sa.Column('CD_ATENDIMENTO', sa.String()),
                                sa.Column('DT_EMISSAO', sa.DateTime),
                                sa.Column('DT_ENTRADA', sa.DateTime),
                                sa.Column('DT_RECEBIMENTO', sa.DateTime),
                                sa.Column('HR_ENTRADA', sa.DateTime),
                                sa.Column('VL_TOTAL', sa.String()),
                                sa.Column('NR_DOCUMENTO', sa.String()),
                                sa.Column('NR_CHAVE_ACESSO', sa.String()),
                                sa.Column('SN_AUTORIZADO', sa.String()),
                                sa.Column('DT_EXTRACAO', sa.DateTime, server_default=sa.text("NOW()")),
                                )
        
    if tabela == lista_tabelas_postgres[10]:     # itsol_com
        id_seq = sa.Sequence(f'id_{tabela}_seq', start=1, increment=1)
        nova_tabela = sa.Table(tabela, metadata,
                               sa.Column(id, sa.BigInteger, id_seq, server_default=id_seq.next_value(), primary_key=True),
                                sa.Column('CD_SOL_COM', sa.String()),
                                sa.Column('CD_PRODUTO', sa.String()),
                                sa.Column('CD_UNI_PRO', sa.String()),
                                sa.Column('CD_MOT_CANCEL', sa.String()),
                                sa.Column('DT_CANCEL', sa.DateTime),
                                sa.Column('QT_SOLIC', sa.String()),
                                sa.Column('QT_COMPRADA', sa.String()),
                                sa.Column('QT_ATENDIDA', sa.String()),
                                sa.Column('SN_COMPRADO', sa.String()),
                                sa.Column('DT_EXTRACAO', sa.DateTime, server_default=sa.text("NOW()")),
                                )

    if tabela == lista_tabelas_postgres[11]:  # ITORD_PRO
        id_seq = sa.Sequence(f'id_{tabela}_seq', start=1, increment=1)
        nova_tabela = sa.Table(tabela, metadata,
                            sa.Column(id, sa.BigInteger, id_seq, server_default=id_seq.next_value(), primary_key=True),
                            sa.Column("CD_ORD_COM", sa.String()),
                            sa.Column("CD_PRODUTO", sa.String()),
                            sa.Column("CD_UNI_PRO", sa.String()),
                            sa.Column("CD_MOT_CANCEL", sa.String()),
                            sa.Column("DT_CANCEL", sa.DateTime),
                            sa.Column("QT_COMPRADA", sa.String()),
                            sa.Column("QT_ATENDIDA", sa.String()),
                            sa.Column("QT_RECEBIDA", sa.String()),
                            sa.Column("QT_CANCELADA", sa.String()),
                            sa.Column("VL_UNITARIO", sa.String()),
                            sa.Column("VL_TOTAL", sa.String()),
                            sa.Column("VL_CUSTO_REAL", sa.String()),
                            sa.Column("VL_TOTAL_CUSTO_REAL", sa.String()),
                            sa.Column("DT_EXTRACAO", sa.DateTime, server_default=sa.text("NOW()")),
                            )

    if tabela == lista_tabelas_postgres[12]:  # ITENT_PRO
        id_seq = sa.Sequence(f'id_{tabela}_seq', start=1, increment=1)
        nova_tabela = sa.Table(tabela, metadata,
                            sa.Column(id, sa.BigInteger, id_seq, server_default=id_seq.next_value(), primary_key=True),
                            sa.Column("CD_ITENT_PRO", sa.String()),
                            sa.Column("CD_ENT_PRO", sa.String()),
                            sa.Column("CD_PRODUTO", sa.String()),
                            sa.Column("CD_UNI_PRO", sa.String()),
                            sa.Column("CD_ATENDIMENTO", sa.String()),
                            sa.Column("CD_CUSTO_MEDIO", sa.String()),
                            sa.Column("CD_PRODUTO_FORNECEDOR", sa.String()),
                            sa.Column("DT_GRAVACAO", sa.DateTime),
                            sa.Column("QT_ENTRADA", sa.String()),
                            sa.Column("QT_DEVOLVIDA", sa.String()),
                            sa.Column("QT_ATENDIDA", sa.String()),
                            sa.Column("VL_UNITARIO", sa.String()),
                            sa.Column("VL_CUSTO_REAL", sa.String()),
                            sa.Column("VL_TOTAL_CUSTO_REAL", sa.String()),
                            sa.Column("VL_TOTAL", sa.String()),
                            sa.Column("DT_EXTRACAO", sa.DateTime, server_default=sa.text("NOW()")),
                            )

    if tabela == lista_tabelas_postgres[13]:  # PRODUTO
        id_seq = sa.Sequence(f'id_{tabela}_seq', start=1, increment=1)
        nova_tabela = sa.Table(tabela, metadata,
                            sa.Column(id, sa.BigInteger, id_seq, server_default=id_seq.next_value(), primary_key=True),
                            sa.Column("CD_PRODUTO", sa.String()),
                            sa.Column("CD_ESPECIE", sa.String()),
                            sa.Column("DS_PRODUTO", sa.String()),
                            sa.Column("DS_PRODUTO_RESUMIDO", sa.String()),
                            sa.Column("DT_CADASTRO", sa.DateTime),
                            sa.Column("DT_ULTIMA_ENTRADA", sa.DateTime),
                            sa.Column("HR_ULTIMA_ENTRADA", sa.DateTime),
                            sa.Column("QT_ESTOQUE_ATUAL", sa.String()),
                            sa.Column("QT_ULTIMA_ENTRADA", sa.String()),
                            sa.Column("VL_ULTIMA_ENTRADA", sa.String()),
                            sa.Column("VL_CUSTO_MEDIO", sa.String()),
                            sa.Column("VL_ULTIMA_CUSTO_REAL", sa.String()),
                            sa.Column("DT_EXTRACAO", sa.DateTime, server_default=sa.text("NOW()")),
                            )

    if tabela == lista_tabelas_postgres[14]:  # EST_PRO
        id_seq = sa.Sequence(f'id_{tabela}_seq', start=1, increment=1)
        nova_tabela = sa.Table(tabela, metadata,
                            sa.Column(id, sa.BigInteger, id_seq, server_default=id_seq.next_value(), primary_key=True),
                            sa.Column("CD_ESTOQUE", sa.String()),
                            sa.Column("CD_PRODUTO", sa.String()),
                            sa.Column("CD_LOCALIZACAO", sa.String()),
                            sa.Column("DS_LOCALIZACAO_PRATELEIRA", sa.String()),
                            sa.Column("DT_ULTIMA_MOVIMENTACAO", sa.DateTime),
                            sa.Column("QT_ESTOQUE_ATUAL", sa.String()),
                            sa.Column("QT_ESTOQUE_MAXIMO", sa.String()),
                            sa.Column("QT_ESTOQUE_MINIMO", sa.String()),
                            sa.Column("QT_ESTOQUE_VIRTUAL", sa.String()),
                            sa.Column("QT_PONTO_DE_PEDIDO", sa.String()),
                            sa.Column("QT_CONSUMO_MES", sa.String()),
                            sa.Column("QT_SOLICITACAO_DE_COMPRA", sa.String()),
                            sa.Column("QT_ORDEM_DE_COMPRA", sa.String()),
                            sa.Column("QT_ESTOQUE_DOADO", sa.String()),
                            sa.Column("QT_ESTOQUE_RESERVADO", sa.String()),
                            sa.Column("QT_CONSUMO_ATUAL", sa.String()),
                            sa.Column("TP_CLASSIFICACAO_ABC", sa.String()),
                            sa.Column("DT_EXTRACAO", sa.DateTime, server_default=sa.text("NOW()")),
                            )

    if tabela == lista_tabelas_postgres[15]:  # mvto_estoque
        id_seq = sa.Sequence(f'id_{tabela}_seq', start=1, increment=1)
        nova_tabela = sa.Table(tabela, metadata,
                            sa.Column(id, sa.BigInteger, id_seq, server_default=id_seq.next_value(), primary_key=True),
                            sa.Column("CD_MVTO_ESTOQUE", sa.String()),
                            sa.Column("CD_ESTOQUE", sa.String()),
                            sa.Column("CD_UNI_PRO", sa.String()),
                            sa.Column("CD_UNID_INT", sa.String()),
                            sa.Column("CD_SETOR", sa.String()),
                            sa.Column("CD_ESTOQUE_DESTINO", sa.String()),
                            sa.Column("CD_CUSTO_MEDIO", sa.String()),
                            sa.Column("CD_AVISO_CIRURGIA", sa.String()),
                            sa.Column("CD_ENT_PRO", sa.String()),
                            sa.Column("CD_USUARIO", sa.String()),
                            sa.Column("CD_FORNECEDOR", sa.String()),
                            sa.Column("CD_PRESTADOR", sa.String()),
                            sa.Column("CD_PRE_MED", sa.String()),
                            sa.Column("CD_ATENDIMENTO", sa.String()),
                            sa.Column("CD_MOT_DEV", sa.String()),
                            sa.Column("DT_MVTO_ESTOQUE", sa.DateTime),
                            sa.Column("HR_MVTO_ESTOQUE", sa.DateTime),
                            sa.Column("VL_TOTAL", sa.String()),
                            sa.Column("TP_MVTO_ESTOQUE", sa.String()),
                            sa.Column("NR_DOCUMENTO", sa.String()),
                            sa.Column("CHAVE_NFE", sa.String()),
                            sa.Column("DT_EXTRACAO", sa.DateTime, server_default=sa.text("NOW()")),
                            )

    if tabela == lista_tabelas_postgres[16]:  # itmvto_estoque
        id_seq = sa.Sequence(f'id_{tabela}_seq', start=1, increment=1)
        nova_tabela = sa.Table(tabela, metadata,
                            sa.Column(id, sa.BigInteger, id_seq, server_default=id_seq.next_value(), primary_key=True),
                            sa.Column("CD_ITMVTO_ESTOQUE", sa.String()),
                            sa.Column("CD_MVTO_ESTOQUE", sa.String()),
                            sa.Column("CD_PRODUTO", sa.String()),
                            sa.Column("CD_UNI_PRO", sa.String()),
                            sa.Column("CD_LOTE", sa.String()),
                            sa.Column("CD_ITENT_PRO", sa.String()),
                            sa.Column("CD_FORNECEDOR", sa.String()),
                            sa.Column("CD_ITPRE_MED", sa.String()),
                            sa.Column("DT_VALIDADE", sa.DateTime),
                            sa.Column("DH_MVTO_ESTOQUE", sa.DateTime),
                            sa.Column("QT_MOVIMENTACAO", sa.String()),
                            sa.Column("VL_UNITARIO", sa.String()),
                            sa.Column("TP_ESTOQUE", sa.String()),
                            sa.Column("DT_EXTRACAO", sa.DateTime, server_default=sa.text("NOW()")),
                            )

    metadata.create_all(engine_postgres, tables=[nova_tabela])


def gerar_texto_querys(query: str, maior_id: int):

    sql_dir = os.path.join(os.path.dirname(__file__), 'raw_mv', 'modelos')
    sql_path = os.path.join(sql_dir, f'{query}.sql')

    with open(sql_path, 'r') as arq:
        texto_query = arq.read()

        if ':MAIOR_ID' not in texto_query:
            texto_query
            
        else:
            if maior_id == 0:
                maior_id = 1
                texto_query = texto_query.replace(':MAIOR_ID', str(maior_id))
            else:
                texto_query = texto_query.replace(':MAIOR_ID', str(maior_id))

        print(texto_query)
        return texto_query


def inserir_dados_postgres(tabela:str, colunas:str, place_insert:str, df):

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

    # query_max_id = f"SELECT * FROM {nome_schema_postgres}.{tabela}"
    # conn.execute(query_max_id)
    # resultado = conn.fetchall()
    # print(tabela)
    # print(resultado)



default_args = {
    'owner' : 'airflow_suprimentos',
    'start_date' : datetime(2024, 1, 1),
    'retries': 0,
    'retry_delay' : timedelta(minutes=1),
}

with DAG(
    dag_id= "Extracao_Suprimentos_MV",
    default_args=default_args,
    description= "Extracao de Dados da Produção p/ Camada Raw - SUPRIMENTOS",
    schedule_interval=timedelta(minutes=20),
    catchup=False,
) as dag:

    lista_tabelas_postgres = ['uni_pro', 'mot_cancel', 'lot_pro', 'fornecedor', 'estoque', 'setor', 'especie', 'sol_com', 'ord_com', 'ent_pro', 'itsol_com', 'itord_pro',
                          'itent_pro', 'produto', 'est_pro', 'mvto_estoque', 'itmvto_estoque' ]


    def extracao_oracle_for_postgres(nome_tabela):

        @task(task_id=f"obter_maior_id_{nome_tabela}")
        def obter_maior_id(nome_tabela: str):

            with postgres_hook.get_conn() as conn_pg:
                cursor_pg = conn_pg.cursor()

                inspector = sa.inspect(engine_postgres)
                bool_table = inspector.has_table(nome_tabela, nome_schema_postgres)

                if bool_table:

                    apagar_tabela(nome_tabela, cursor_pg) if nome_tabela in ['produto', 'est_pro'] else None

                    query_max_id = f"SELECT MAX(id_{nome_tabela}) FROM {nome_schema_postgres}.{nome_tabela}"
                    cursor_pg.execute(query_max_id)
                    resultado = cursor_pg.fetchone()[0]

                    maior_id = resultado if resultado is not None else 0

                else:
                    
                    criar_tabela_raw(nome_tabela, cursor_pg)
                    maior_id = 0

                cursor_pg.close()
                return maior_id


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

        
        maior_id_task = obter_maior_id(nome_tabela)
        extrair_dados_oracle_task = extrair_dados_oracle(nome_tabela, maior_id_task)

        maior_id_task >> extrair_dados_oracle_task

        return [maior_id_task, extrair_dados_oracle_task]

    tarefas_extracao = []
    for nome_tabela in lista_tabelas_postgres:
        tarefas_extracao.extend(extracao_oracle_for_postgres(nome_tabela))

    cosmos_dag_suprimentos = DbtTaskGroup(
        group_id="dbt_trf_suprimentos",
        project_config=ProjectConfig(suprimentos_path),
        profile_config=airflow_postgres_db,
        execution_config=venv_execution_config,
    )

    for tarefa in tarefas_extracao:
        tarefa >> cosmos_dag_suprimentos