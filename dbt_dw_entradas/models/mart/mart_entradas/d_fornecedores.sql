{{
    config(
        materialized = 'incremental',
        unique_key = 'cd_fornecedor',
        on_schema_change = 'sync_all_columns',
        tags = ['entradas']
    )
}}

WITH source_fornecedores
    AS (
        SELECT
          cd_fornecedor,
          cd_cidade,
          cd_setor,
          nm_fornecedor,
          nm_fantasia,
          nr_cgc_cpf AS cnpj
        FROM {{ ref( 'stg_fornecedores' ) }}
),
treats
    AS (
        SELECT
            *
        FROM source_fornecedores

)
SELECT * FROM treats
