{{
    config(
        materialized = 'incremental',
    unique_key = 'cd_fornecedor',
        on_schema_change = 'sync_all_columns',
        tags = ['entradas']
    )
}}

WITH source_fornecedor
    AS (
        SELECT
          cd_fornecedor,
          cd_cidade,
          cd_setor,
          nm_fornecedor,
          nm_fantasia,
          nr_cgc_cpf
        FROM {{ source('raw_entradas_mv', 'fornecedor') }}
),
treats
    AS (
        SELECT
          cd_fornecedor::BIGINT,
          cd_cidade::BIGINT,
          cd_setor::BIGINT,
          nm_fornecedor::VARCHAR(250),
          nm_fantasia::VARCHAR(100),
          nr_cgc_cpf::VARCHAR(60)
        FROM source_fornecedor
)
SELECT * FROM treats