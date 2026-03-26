{{
    config( materialized = 'incremental',
            unique_key = 'cd_gru_pro',
        on_schema_change = 'sync_all_columns',
        tags = ['repasse']
    )
}}

WITH source_gru_pro
    AS (
        SELECT
            sis.cd_gru_pro,
            sis.ds_gru_pro
        FROM {{ source('raw_repasse_mv', 'gru_pro')}} sis
        {% if is_incremental() %}
        WHERE sis.cd_gru_pro::BIGINT > ( SELECT MAX(cd_gru_pro) FROM {{this}} )
        {% endif %}
),
treats
    AS (
        SELECT
            cd_gru_pro::BIGINT,
            ds_gru_pro::VARCHAR(40)
        FROM source_gru_pro
)
SELECT * FROM treats