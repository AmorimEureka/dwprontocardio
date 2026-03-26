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

    def is_datetime_column(column_name: str) -> bool:
        return bool(column_name) and ('DT_' in column_name or 'DATA' in column_name or 'HR_' in column_name)

    def parse_datetime_value(value):
        if isinstance(value, datetime):
            return value

        if isinstance(value, str):
            valor = value.strip()
            if not valor:
                return None

            formatos = (
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%d',
                '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%dT%H:%M:%S.%f',
            )
            for formato in formatos:
                try:
                    return datetime.strptime(valor, formato)
                except ValueError:
                    continue

            try:
                return datetime.fromisoformat(valor)
            except ValueError:
                return None

        return None

    def get_table_config(config_key: str):
        valor = dlt.config.get(f"sources.sql_database.{tabela}.{config_key}", None)
        if valor is None:
            valor = dlt.config.get(f"source.sql_database.{tabela}.{config_key}", None)
        return valor

    # Obter config's do arquivo de 'config.toml' do dlt
    config_campos_tabelas = get_table_config("included_columns")
    config_cursor_incremental = get_table_config("incremental_column")
    config_modo_escrita = get_table_config("write_disposition")
    config_primary_key = get_table_config("primary_key")
    config_valor_inicial = get_table_config("initial_value")
    incremental_is_datetime = is_datetime_column(config_cursor_incremental)

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
        if is_datetime_column(campo):
            campos_select.append(f"TO_CHAR({campo}, 'YYYY-MM-DD HH24:MI:SS') as {campo}")
        else:
            campos_select.append(campo)

    campos_tabelas = ", ".join(campos_select)

    @dlt.resource(name=tabela, write_disposition=config_modo_escrita, primary_key=config_primary_key)
    def recursos_dinamicos(**kwargs):

        # Defini cursor p/ carregamento incremental passando o 'campo incremental'
        # ou primeira data de extracao quando for primeira extracao
        resource_state = dlt.current.resource_state()
        cursor_incremental = kwargs.get(
            config_cursor_incremental,
            resource_state.get(config_cursor_incremental, valor_inicial)
        )

        if incremental_is_datetime:
            cursor_incremental = parse_datetime_value(cursor_incremental) or valor_inicial

        if tabela == "PRO_FAT" and config_cursor_incremental == "CD_PRO_FAT_NUM":
            if isinstance(cursor_incremental, str):
                cursor_incremental = int(cursor_incremental) if cursor_incremental.isdigit() else valor_inicial
            elif not isinstance(cursor_incremental, (int, float)):
                cursor_incremental = valor_inicial

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
                if tabela == "PRO_FAT" and config_cursor_incremental == "CD_PRO_FAT_NUM":
                    bind_ultimo_valor = cursor.var(ora.NUMBER)
                elif incremental_is_datetime or isinstance(cursor_incremental, datetime):
                    bind_ultimo_valor = cursor.var(ora.DATETIME)
                elif isinstance(cursor_incremental, (int, float)):
                    bind_ultimo_valor = cursor.var(ora.NUMBER)
                else:
                    bind_ultimo_valor = cursor.var(ora.STRING)

                bind_ultimo_valor.setvalue(0, cursor_incremental)

                if tabela == "PRO_FAT" and config_cursor_incremental == "CD_PRO_FAT_NUM":
                    consulta = f"""
                        WITH base AS (
                            SELECT
                                {campos_tabelas},
                                CASE
                                    WHEN REGEXP_LIKE(TRIM(CD_PRO_FAT), '^[[:digit:]]+$')
                                    THEN TO_NUMBER(TRIM(CD_PRO_FAT))
                                    ELSE NULL
                                END AS CD_PRO_FAT_NUM
                            FROM DBAMV.{tabela}
                        )
                        SELECT
                            {campos_tabelas},
                            CD_PRO_FAT_NUM
                        FROM base
                        WHERE TRIM(CD_PRO_FAT) = 'X0000000'
                           OR CD_PRO_FAT_NUM >= :last_value
                        ORDER BY NVL(CD_PRO_FAT_NUM, -1) ASC
                    """
                else:
                    consulta = f"""
                        SELECT {campos_tabelas}
                        FROM DBAMV.{tabela}
                        WHERE {config_cursor_incremental} >= :last_value
                        ORDER BY {config_cursor_incremental} ASC
                    """

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
                            if valor and isinstance(valor, str) and is_datetime_column(campo):
                                try:
                                    # Tentar converter string para datetime
                                    row_dict[campo] = datetime.strptime(valor, '%Y-%m-%d %H:%M:%S')
                                except (ValueError, TypeError):
                                    row_dict[campo] = None

                        valor_cursor = row_dict.get(config_cursor_incremental)
                        if incremental_is_datetime:
                            valor_cursor = parse_datetime_value(valor_cursor)
                        if valor_cursor is not None:
                            resource_state[config_cursor_incremental] = valor_cursor

                        if tabela == "PRO_FAT" and config_cursor_incremental == "CD_PRO_FAT_NUM":
                            row_dict.pop("CD_PRO_FAT_NUM", None)

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
def source_acumula_resources(table_names=None, tag=None):

    tabelas_config = dlt.config.get("sources.sql_database.table_names") or []

    if table_names:
        tabelas = table_names
    elif tag:
        tabelas = []
        for tabela in tabelas_config:
            tags_tabela = dlt.config.get(f"sources.sql_database.{tabela}.dag_tags", None)
            if tags_tabela is None:
                tags_tabela = dlt.config.get(f"source.sql_database.{tabela}.dag_tags", [])

            if isinstance(tags_tabela, str):
                tags_tabela = [tags_tabela]

            if tag in (tags_tabela or []):
                tabelas.append(tabela)
    else:
        tabelas = tabelas_config

    yield from [gera_recursos(tabela) for tabela in tabelas]
