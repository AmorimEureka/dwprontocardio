{{
    config(
        materialized = 'incremental',
        unique_key = 'cd_oficina',
        on_schema_change =  'sync_all_columns',
        tags = ['entradas']
    )
}}

WITH source_ocifina
    AS (
        SELECT
            cd_oficina,
            cd_setor,
            ds_oficina
        FROM {{ source('raw_entradas_mv', 'oficina') }}
),
treats
    AS (
        SELECT
            cd_oficina::BIGINT,
            cd_setor::BIGINT,
            ds_oficina::VARCHAR(35)
        FROM source_ocifina
)
SELECT * FROM treats