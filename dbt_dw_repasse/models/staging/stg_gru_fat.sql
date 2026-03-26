{{
    config( materialized = 'incremental',
            unique_key = 'cd_gru_fat',
        on_schema_change = 'sync_all_columns',
        tags = ['repasse']
    )
}}

WITH source_gru_fat
    AS (
        SELECT
            sis.cd_gru_fat,
            sis.ds_gru_fat
        FROM {{ source('raw_repasse_mv', 'gru_fat')}} sis
        {% if is_incremental() %}
        WHERE sis.cd_gru_fat::BIGINT > ( SELECT MAX(cd_gru_fat) FROM {{this}} )
        {% endif %}
),
treats
    AS (
        SELECT
            cd_gru_fat::BIGINT,
            ds_gru_fat::VARCHAR(40)
        FROM source_gru_fat
)
SELECT * FROM treats