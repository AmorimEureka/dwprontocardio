{{
    config( materialized = 'incremental',
            unique_key = '"CD_GRU_FAT"' )
}}

WITH source_gru_fat
    AS (
        SELECT
            NULLIF(sis."CD_GRU_FAT", 'NaN') AS "CD_GRU_FAT",
            sis."DS_GRU_FAT",
            sis."DT_EXTRACAO"
        FROM {{ source('raw_mv', 'gru_fat')}} sis
        {% if is_incremental() %}
        WHERE sis."CD_GRU_FAT"::BIGINT > ( SELECT MAX("CD_GRU_FAT") FROM {{this}} )
        {% endif %}
),
treats
    AS (
        SELECT
            "CD_GRU_FAT"::BIGINT,
            "DS_GRU_FAT"::VARCHAR(40),
            "DT_EXTRACAO"::TIMESTAMP
        FROM source_gru_fat
)
SELECT * FROM treats