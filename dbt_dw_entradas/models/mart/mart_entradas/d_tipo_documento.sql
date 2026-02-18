{{
    config(
        materialized = 'incremental',
        unique_key = 'cd_tip_doc',
        on_schema_change = 'sync_all_columns',
        tags = ['entradas']
    )
}}

WITH source_tipo_documento
    AS (
        SELECT
            cd_tip_doc,
            ds_tip_doc
        FROM {{ ref( 'stg_tipo_documento' ) }}
),
treats
    AS (
        SELECT
            *
        FROM source_tipo_documento

)
SELECT * FROM treats
