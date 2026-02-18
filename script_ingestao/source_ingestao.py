import dlt
import oracledb as ora
from datetime import datetime

import os
from dotenv import load_dotenv

load_dotenv()

# Configuracao p/ dlt usar local correto em producao
os.environ["DLT_PROJECT_DIR"] = "/usr/local/airflow"

# Inicializa conexao oracle modo thick
ora.init_oracle_client(lib_dir="/usr/local/airflow/.oracle/instantclient_19_23")


# Funcao que criar contexto p/ os recursos dlt
def gera_recursos(tabela: str):

    # Obter config's do arquivo de 'config.toml' do dlt
    config_campos_tabelas = dlt.config.get(f"sources.sql_database.{tabela}.included_columns")
    config_cursor_incremental = dlt.config.get(f"sources.sql_database.{tabela}.incremental_column")
    config_modo_escrita = dlt.config.get(f"sources.sql_database.{tabela}.write_disposition")
    config_primary_key = dlt.config.get(f"sources.sql_database.{tabela}.primary_key")
    config_valor_inicial = dlt.config.get(f"sources.sql_database.{tabela}.initial_value")

    # TRATAMENTO: Tipando 'Valor Inicial' da primeira extracao
    if isinstance(config_valor_inicial, str):
        valor_inicial = datetime.strptime(config_valor_inicial, "%Y-%m-%d")
    elif isinstance(config_valor_inicial, (int, float)):
        valor_inicial = config_valor_inicial
    else:
        valor_inicial = config_valor_inicial

    # TRATAMENTO:
    #   ⛧ Prepa a lista de campos
    #   ⛧ Aplicar TO_CHAR() p/ campos de datas
    campos_select = []
    for campo in config_campos_tabelas:
        # Para campos de data conhecidos, usar TO_CHAR
        if 'DT_' in campo or 'DATA' in campo:
            campos_select.append(f"TO_CHAR({campo}, 'YYYY-MM-DD HH24:MI:SS') as {campo}")
        else:
            campos_select.append(campo)

    campos_tabelas = ", ".join(campos_select)

    @dlt.resource(name=tabela, write_disposition=config_modo_escrita, primary_key=config_primary_key)
    def recursos_dinamicos(**kwargs):

        # Defini cursor p/ carregamento incremental passando o 'campo incremental'
        # ou primeira data de extracao quando for primeira extracao
        cursor_incremental = kwargs.get(config_cursor_incremental, valor_inicial)

        conn = None

        try:
            oracle_user = os.getenv("ORACLE_USER")
            oracle_host = os.getenv("ORACLE_HOST")
            oracle_port = os.getenv("ORACLE_PORT")
            oracle_service = os.getenv("ORACLE_SERVICE")

            # Construir DSN no formato correto
            dsn = f"{oracle_host}:{oracle_port}/{oracle_service}"
            print(f"  - DSN: {dsn}")

            conn = ora.connect(
                user=oracle_user,
                password=os.getenv("ORACLE_PASSWORD"),
                dsn=dsn
            )

            with conn.cursor() as cursor:

                # Define tipo o 'bind variable' pelo tipo do cursor_incremental
                if isinstance(cursor_incremental, datetime):
                    bind_ultimo_valor = cursor.var(ora.DATETIME)
                elif isinstance(cursor_incremental, (int, float)):
                    bind_ultimo_valor = cursor.var(ora.NUMBER)
                else:
                    bind_ultimo_valor = cursor.var(ora.STRING)

                bind_ultimo_valor.setvalue(0, cursor_incremental)

                consulta = f"""
                        SELECT {campos_tabelas}
                        FROM DBAMV.{tabela}
                        WHERE {config_cursor_incremental} >= :last_value
                        ORDER BY {config_cursor_incremental} ASC
                    """
                                         # Faz como se fosse um replace no placehold ':last_value'
                cursor.execute(consulta, last_value=bind_ultimo_valor)

                nomes_campos = [campo[0] for campo in cursor.description]

                # Processar linha por linha com tratamento de erro
                while True:
                    try:
                        row = cursor.fetchone()
                        if row is None:
                            break

                        # Criar dicionário da linha
                        row_dict = dict(zip(nomes_campos, row))

                        # Converter strings de data de volta para datetime
                        for campo, valor in list(row_dict.items()):
                            if valor and isinstance(valor, str) and ('DT_' in campo or 'DATA' in campo):
                                try:
                                    # Tentar converter string para datetime
                                    row_dict[campo] = datetime.strptime(valor, '%Y-%m-%d %H:%M:%S')
                                except (ValueError, TypeError) as e:
                                    row_dict[campo] = None

                        # print(f"[DEBUG] da linha extraida: {row_dict}")
                        yield row_dict
                    except ValueError as e:
                        if "year" in str(e) and "out of range" in str(e):
                            print(f"[AVISO] Registro com data inválida encontrado: {e}")
                            print("[AVISO] Pulando registro com erro de data. Continuando processamento...")
                            continue
                        else:
                            raise

        except Exception as e:
            print(f"Error ao tentar conectar com o Oracle:\n {e}")
            raise

        finally:
            if conn:
                conn.close()

    return recursos_dinamicos


@dlt.source
def source_acumula_resources():

    tabelas = dlt.config.get("sources.sql_database.table_names")

    yield from [gera_recursos(tabela) for tabela in tabelas]
