{{
    config( materialized = 'incremental',
            unique_key = 'cd_ati_med',
        on_schema_change = 'sync_all_columns',
        tags = ['repasse']
    )
}}

WITH source_ati_med
    AS (
        SELECT
            sis.cd_ati_med,
            sis.ds_ati_med
        FROM {{ source('raw_repasse_mv', 'ati_med')}} sis
        {% if is_incremental() %}
        WHERE sis.cd_ati_med::BIGINT > ( SELECT MAX(cd_ati_med) FROM {{this}} )
        {% endif %}
),
treats
    AS (
        SELECT
            cd_ati_med::BIGINT,
            ds_ati_med::VARCHAR(30)
        FROM source_ati_med
)
SELECT * FROM treats