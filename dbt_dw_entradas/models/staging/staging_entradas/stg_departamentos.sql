{{
    config(
        materialized = 'incremental',
        unique_key = 'cd_setor',
        on_schema_change = 'sync_all_columns',
        tags = ['entradas']
    )
}}

WITH source_setor
    AS (
        SELECT
            cd_setor,
            nm_setor,
            cd_cen_cus
        FROM {{ source('raw_entradas_mv', 'setor') }}
),
treats
    AS (
        SELECT
            cd_setor::BIGINT,
            nm_setor::VARCHAR(70),
            cd_cen_cus::BIGINT
        FROM source_setor
)
SELECT * FROM treats