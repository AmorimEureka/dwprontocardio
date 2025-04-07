{{
    config( materialized = 'incremental',
            unique_key = '"CD_ATI_MED"' )
}}

WITH source_ati_med
    AS (
        SELECT
            sis."CD_ATI_MED",
            sis."DS_ATI_MED",
            sis."DT_EXTRACAO"
        FROM {{ source('raw_mv', 'ati_med')}} sis
        {% if is_incremental() %}
        WHERE sis."CD_ATI_MED"::BIGINT > ( SELECT MAX("CD_ATI_MED") FROM {{this}} )
        {% endif %}
),
treats
    AS (
        SELECT
            "CD_ATI_MED"::BIGINT,
            "DS_ATI_MED"::VARCHAR(30),
            "DT_EXTRACAO"::TIMESTAMP
        FROM source_ati_med
)
SELECT * FROM treats