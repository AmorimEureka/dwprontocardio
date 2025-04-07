{{
    config( materialized = 'incremental',
            unique_key = '"CD_GRU_PRO"' )
}}

WITH source_gru_pro
    AS (
        SELECT
            NULLIF(sis."CD_GRU_PRO", 'NaN') AS "CD_GRU_PRO",
            sis."DS_GRU_PRO",
            sis."DT_EXTRACAO"
        FROM {{ source('raw_mv', 'gru_pro')}} sis
        {% if is_incremental() %}
        WHERE sis."CD_GRU_PRO"::BIGINT > ( SELECT MAX("CD_GRU_PRO") FROM {{this}} )
        {% endif %}
),
treats
    AS (
        SELECT
            "CD_GRU_PRO"::BIGINT,
            "DS_GRU_PRO"::VARCHAR(40),
            "DT_EXTRACAO"::TIMESTAMP
        FROM source_gru_pro
)
SELECT * FROM treats