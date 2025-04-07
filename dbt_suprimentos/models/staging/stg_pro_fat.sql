{{
    config( materialized = 'incremental',
            unique_key = '"CD_PRO_FAT"' )
}}

WITH source_pro_fat
    AS (
        SELECT
            sis."CD_PRO_FAT",
            sis."CD_GRU_PRO",
            sis."DS_PRO_FAT",
            sis."DT_EXTRACAO"
        FROM {{ source('raw_mv', 'pro_fat')}} sis
        {% if is_incremental() %}
        WHERE sis."DT_EXTRACAO"::TIMESTAMP > ( SELECT MAX("DT_EXTRACAO") FROM {{ this }} )
        {% endif %}
),
treats
    AS (
        SELECT
            "CD_PRO_FAT"::VARCHAR(21),
            "CD_GRU_PRO"::BIGINT,
            "DS_PRO_FAT"::VARCHAR(250),
            "DT_EXTRACAO"::TIMESTAMP
        FROM source_pro_fat
)
SELECT * FROM treats