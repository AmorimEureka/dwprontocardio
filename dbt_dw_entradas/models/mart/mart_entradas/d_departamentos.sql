{{
    config(
        materialized = 'incremental',
        unique_key = 'cd_setor',
        on_schema_change = 'sync_all_columns',
        tags = ['entradas']
    )
}}

WITH source_departamentos
    AS (
        SELECT
            cd_setor,
            nm_setor
        FROM {{ ref( 'stg_departamentos' ) }}
),
treats
    AS (
        SELECT
            *
        FROM source_departamentos

)
SELECT * FROM treats
