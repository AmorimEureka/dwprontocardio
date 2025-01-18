from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from cosmos import DbtTaskGroup, ProjectConfig
from include.profiles import airflow_postgres_db
from include.constants import suprimentos_path, venv_execution_config

import os
import sqlalchemy as sa
from datetime import datetime, timedelta
import pandas as pd
# from alembic.config import Config
# from alembic import command


postgres_hook = PostgresHook(postgres_conn_id='postgres_prontocardio')

engine_postgres = sa.create_engine(postgres_hook.get_uri())

nome_schema_postgres = 'raw_mv'

oracle_hook = OracleHook(oracle_conn_id='oracle_prontocardio', thick_mode=True)




def criar_tabela_raw():

    metadata = sa.MetaData(schema=nome_schema_postgres)

    # id = f"id_{tabela}"

    # if tabela == lista_tabelas_postgres[0]:     # uni_pro
    id_seq1 = sa.Sequence(f'id_uni_pro_seq', start=1, increment=1)
    nova_tabela1 = sa.Table('uni_pro', metadata,
                            sa.Column('id_uni_pro', sa.BigInteger, id_seq1, server_default=id_seq1.next_value(), primary_key=True),
                            sa.Column('CD_UNI_PRO', sa.String()),
                            sa.Column('CD_UNIDADE', sa.String()),
                            sa.Column('CD_PRODUTO', sa.String()),
                            sa.Column('VL_FATOR', sa.String()),
                            sa.Column('DS_UNIDADE', sa.String()),
                            sa.Column('TP_RELATORIOS', sa.String()),
                            sa.Column('SN_ATIVO', sa.String()),
                            sa.Column('DT_EXTRACAO', sa.DateTime, server_default=sa.text("NOW()")),
                            )
    
    # if tabela == lista_tabelas_postgres[1]:     # mot_cancel
    id_seq2 = sa.Sequence(f'id_mot_cancel_seq', start=1, increment=1)
    nova_tabela2 = sa.Table('mot_cancel', metadata,
                            sa.Column('id_mot_cancel', sa.BigInteger, id_seq2, server_default=id_seq2.next_value(), primary_key=True),
                            sa.Column('CD_MOT_CANCEL', sa.String()),
                            sa.Column('DS_MOT_CANCEL', sa.String()),
                            sa.Column('TP_MOT_CANCEL', sa.String()),
                            sa.Column('DT_EXTRACAO', sa.DateTime, server_default=sa.text("NOW()")),
                            )
        
    # if tabela == lista_tabelas_postgres[2]:     # lot_pro
    id_seq3 = sa.Sequence(f'id_lot_pro_seq', start=1, increment=1)
    nova_tabela3 = sa.Table('lot_pro', metadata,
                            sa.Column('id_lot_pro', sa.BigInteger, id_seq3, server_default=id_seq3.next_value(), primary_key=True),
                            sa.Column('CD_LOT_PRO', sa.String()),
                            sa.Column('CD_ESTOQUE', sa.String()),
                            sa.Column('CD_PRODUTO', sa.String()),
                            sa.Column('CD_LOTE', sa.String()),
                            sa.Column('DT_VALIDADE', sa.DateTime),
                            sa.Column('QT_ESTOQUE_ATUAL', sa.String()),
                            sa.Column('DT_EXTRACAO', sa.DateTime, server_default=sa.text("NOW()")),
                            )
        
    # if tabela == lista_tabelas_postgres[3]:     # fornecedor
    id_seq4 = sa.Sequence(f'id_fornecedor_seq', start=1, increment=1)
    nova_tabela4 = sa.Table('fornecedor', metadata,
                            sa.Column('id_fornecedor', sa.BigInteger, id_seq4, server_default=id_seq4.next_value(), primary_key=True),
                            sa.Column('CD_FORNECEDOR', sa.String()),
                            sa.Column('NM_FORNECEDOR', sa.String(255)),
                            sa.Column('NM_FANTASIA', sa.String(255)),
                            sa.Column('DT_INCLUSAO', sa.DateTime),
                            sa.Column('NR_CGC_CPF', sa.String()),
                            sa.Column('TP_FORNECEDOR', sa.String()),
                            sa.Column('DT_EXTRACAO', sa.DateTime, server_default=sa.text("NOW()")),
                            )
        
    # if tabela == lista_tabelas_postgres[4]:     # estoque
    id_seq5 = sa.Sequence(f'id_estoque_seq', start=1, increment=1)
    nova_tabela5 = sa.Table('estoque', metadata,
                            sa.Column('id_estoque', sa.BigInteger, id_seq5, server_default=id_seq5.next_value(), primary_key=True),
                            sa.Column('CD_ESTOQUE', sa.String()),
                            sa.Column('CD_SETOR', sa.String()),
                            sa.Column('DS_ESTOQUE', sa.String()),
                            sa.Column('TP_ESTOQUE', sa.String()),
                            sa.Column('DT_EXTRACAO', sa.DateTime, server_default=sa.text("NOW()")),
                            )
        
    # if tabela == lista_tabelas_postgres[5]:     # setor
    id_seq6 = sa.Sequence(f'id_setor_seq', start=1, increment=1)
    nova_tabela6 = sa.Table('setor', metadata,
                            sa.Column('id_setor', sa.BigInteger, id_seq6, server_default=id_seq6.next_value(), primary_key=True),
                            sa.Column('CD_SETOR', sa.String()),
                            sa.Column('CD_FATOR', sa.String()),
                            sa.Column('CD_GRUPO_DE_CUSTO', sa.String()),
                            sa.Column('CD_SETOR_CUSTO', sa.String()),
                            sa.Column('NM_SETOR', sa.String()),
                            sa.Column('SN_ATIVO', sa.String()),
                            sa.Column('DT_EXTRACAO', sa.DateTime, server_default=sa.text("NOW()")),
                            )
        
    # if tabela == lista_tabelas_postgres[6]:     # especie
    id_seq7 = sa.Sequence(f'id_especie_seq', start=1, increment=1)
    nova_tabela7 = sa.Table('especie', metadata,
                            sa.Column('id_especie', sa.BigInteger, id_seq7, server_default=id_seq7.next_value(), primary_key=True),
                            sa.Column('CD_ESPECIE', sa.String()),
                            sa.Column('CD_ITEM_RES', sa.String()),
                            sa.Column('DS_ESPECIE', sa.String()),
                            sa.Column('DT_EXTRACAO', sa.DateTime, server_default=sa.text("NOW()")),
                            )
        
    # if tabela == lista_tabelas_postgres[7]:     # sol_com
    id_seq8 = sa.Sequence(f'id_sol_com_seq', start=1, increment=1)
    nova_tabela8 = sa.Table('sol_com', metadata,
                            sa.Column('id_sol_com', sa.BigInteger, id_seq8, server_default=id_seq8.next_value(), primary_key=True),
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
        
    # if tabela == lista_tabelas_postgres[8]:     # ord_com
    id_seq9 = sa.Sequence(f'id_ord_com_seq', start=1, increment=1)
    nova_tabela9 = sa.Table('ord_com', metadata,
                            sa.Column('id_ord_com', sa.BigInteger, id_seq9, server_default=id_seq9.next_value(), primary_key=True),
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
        
    # if tabela == lista_tabelas_postgres[9]:     # ent_pro
    id_seq10 = sa.Sequence(f'id_ent_pro_seq', start=1, increment=1)
    nova_tabela10 = sa.Table('ent_pro', metadata,
                            sa.Column('id_ent_pro', sa.BigInteger, id_seq10, server_default=id_seq10.next_value(), primary_key=True),
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
        
    # if tabela == lista_tabelas_postgres[10]:     # itsol_com
    id_seq11 = sa.Sequence(f'id_itsol_com_seq', start=1, increment=1)
    nova_tabela11 = sa.Table('itsol_com', metadata,
                            sa.Column('id_itsol_com', sa.BigInteger, id_seq11, server_default=id_seq11.next_value(), primary_key=True),
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
        
    # if tabela == lista_tabelas_postgres[11]:  # ITORD_PRO
    id_seq12 = sa.Sequence(f'id_itord_pro_seq', start=1, increment=1)
    nova_tabela12 = sa.Table('itord_pro', metadata,
                        sa.Column('id_itord_pro', sa.BigInteger, id_seq12, server_default=id_seq12.next_value(), primary_key=True),
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
        
    # if tabela == lista_tabelas_postgres[12]:  # ITENT_PRO
    id_seq13 = sa.Sequence(f'id_itent_pro_seq', start=1, increment=1)
    nova_tabela13 = sa.Table('itent_pro', metadata,
                        sa.Column('id_itent_pro', sa.BigInteger, id_seq13, server_default=id_seq13.next_value(), primary_key=True),
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
        
    # if tabela == lista_tabelas_postgres[13]:  # PRODUTO
    id_seq14 = sa.Sequence(f'id_produto_seq', start=1, increment=1)
    nova_tabela14 = sa.Table('produto', metadata,
                        sa.Column('id_produto', sa.BigInteger, id_seq14, server_default=id_seq14.next_value(), primary_key=True),
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
        
    # if tabela == lista_tabelas_postgres[14]:  # EST_PRO
    id_seq15 = sa.Sequence(f'id_est_pro_seq', start=1, increment=1)
    nova_tabela15 = sa.Table('est_pro', metadata,
                        sa.Column('id_est_pro', sa.BigInteger, id_seq15, server_default=id_seq15.next_value(), primary_key=True),
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
        
    # if tabela == lista_tabelas_postgres[15]:  # mvto_estoque
    id_seq16 = sa.Sequence(f'id_mvto_estoque_seq', start=1, increment=1)
    nova_tabela16 = sa.Table('mvto_estoque', metadata,
                        sa.Column('id_mvto_estoque', sa.BigInteger, id_seq16, server_default=id_seq16.next_value(), primary_key=True),
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
        
    # if tabela == lista_tabelas_postgres[16]:  # itmvto_estoque
    id_seq17 = sa.Sequence(f'id_itmvto_estoque_seq', start=1, increment=1)
    nova_tabela17 = sa.Table('itmvto_estoque', metadata,
                        sa.Column('id_itmvto_estoque', sa.BigInteger, id_seq17, server_default=id_seq17.next_value(), primary_key=True),
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
        
    metadata.create_all(engine_postgres, tables=[nova_tabela1, nova_tabela2, nova_tabela3, nova_tabela4, nova_tabela5, nova_tabela6, nova_tabela7, nova_tabela8, nova_tabela9, nova_tabela10, nova_tabela11, nova_tabela12, nova_tabela13, nova_tabela14, nova_tabela15, nova_tabela16, nova_tabela17])


# def apply_migrations():
#     try:
#         alembic_cfg_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "alembic.ini")
#         if not os.path.exists(alembic_cfg_path):
#             raise FileNotFoundError(f"Arquivo alembic.ini não encontrado no caminho: {alembic_cfg_path}")
        
#         alembic_cfg = Config(alembic_cfg_path)
#         command.upgrade(alembic_cfg, "head")
#     except Exception as e:
#         print(f"Erro ao aplicar migrações: {e}")
#         raise


def gerar_texto_querys(query: str, where: str):

    sql_dir = os.path.join(os.path.dirname(__file__), 'raw_mv', 'modelos')
    sql_path = os.path.join(sql_dir, f'{query}.sql')


    with open(sql_path, 'r') as arq:

        texto_query = arq.read()

        if ':PARAM' not in texto_query:
            texto_query
            
        else:
            if where == 0 :
                where = 1
                texto_query = texto_query.replace(':PARAM', str(where))
            else:
                texto_query = texto_query.replace(':PARAM', str(where))

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
    
    lista_tabelas_postgres = {
        'lista_tab_incremental' : ['uni_pro', 'mot_cancel', 'lot_pro', 'fornecedor', 'estoque', 'setor', 'especie', 'mvto_estoque', 'itmvto_estoque', 'itsol_com', 'itord_pro', 'itent_pro'] ,
        'lista_tab_truncate' : ['produto', 'est_pro'] ,
        'lista_tab_snapshot' : ['sol_com', 'ord_com', 'ent_pro']
    }


    # apply_migrations_task = PythonOperator(
    #     task_id='apply_migrations',
    #     python_callable=apply_migrations
    # )


    criar_tabela_raw_task = PythonOperator(
        task_id='criar_tabela_raw',
        python_callable=criar_tabela_raw
    )


    def extracao_oracle_for_postgres_incremental(nome_tabela):

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

                    # criar_tabela_raw(nome_tabela, cursor_pg)
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
    


    def extracao_oracle_for_postgres_snapshot(nome_tabela):

        @task(task_id=f"obter_maior_dt_{nome_tabela}")
        def obter_maior_dt(nome_tabela: str):

            tabela = nome_tabela
            
            with postgres_hook.get_conn() as conn_pg:
                cursor_pg = conn_pg.cursor()

                count_query = f"SELECT COUNT(id_{tabela}) FROM {nome_schema_postgres}.{tabela}"
                cursor_pg.execute(count_query)
                resultado = cursor_pg.fetchone()[0]
                
                resultado = resultado if resultado is not None else 0
                if resultado != 0:
                    if nome_tabela == 'ent_pro':
                        nome_tabela = 'entrada'
                    param_maior_dt_query = f"SELECT MAX(\"DT_{nome_tabela.upper()}\") FROM {nome_schema_postgres}.{tabela}"
                    cursor_pg.execute(param_maior_dt_query)
                    maior_dt_query = cursor_pg.fetchone()[0]
                    maior_dt_query = str(maior_dt_query).split()[0]
                    apagar_tabela(tabela, cursor_pg)
                else:
                    maior_dt_query = '2023-01-01'

                cursor_pg.close()

                return maior_dt_query


        @task(task_id=f"extrair_dados_oracle_{nome_tabela}")
        def extrair_dados_oracle(nome_tabela: str, maior_dt: str):

            with oracle_hook.get_conn() as conn_ora:

                cursor_ora = conn_ora.cursor()
                texto_query = gerar_texto_querys(nome_tabela, maior_dt)
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

        maior_dt_task = obter_maior_dt(nome_tabela)
        extrair_dados_oracle_task = extrair_dados_oracle(nome_tabela, maior_dt_task)
        maior_dt_task >> extrair_dados_oracle_task
        return [maior_dt_task, extrair_dados_oracle_task]

    tarefas_extracao = []
    for lista in lista_tabelas_postgres:
        if lista == 'lista_tab_incremental':
            for nome_tabela in lista_tabelas_postgres['lista_tab_incremental']:
                tarefas_extracao.extend(extracao_oracle_for_postgres_incremental(nome_tabela))
        if lista == 'lista_tab_snapshot':
            for nome_tabela in lista_tabelas_postgres['lista_tab_snapshot']:
                tarefas_extracao.extend(extracao_oracle_for_postgres_snapshot(nome_tabela))
        if lista == 'lista_tab_truncate':
            for nome_tabela in lista_tabelas_postgres['lista_tab_truncate']:
                tarefas_extracao.extend(extracao_oracle_for_postgres_incremental(nome_tabela))

    for tarefa in tarefas_extracao:
        criar_tabela_raw_task >> tarefa

    cosmos_dag_suprimentos = DbtTaskGroup(
        group_id="dbt_trf_suprimentos",
        project_config=ProjectConfig(suprimentos_path),
        profile_config=airflow_postgres_db,
        execution_config=venv_execution_config,
    )

    for tarefa in tarefas_extracao:
        tarefa >> cosmos_dag_suprimentos