{{
    config( materialized = 'incremental',
            unique_key = '"CD_REG_FAT"' )
}}

WITH source_reg_fat
    AS (
        SELECT
            NULLIF(sis."CD_REG_FAT", 'NaN') AS "CD_REG_FAT",
            NULLIF(sis."CD_CONVENIO", 'NaN') AS "CD_CONVENIO",
            NULLIF(sis."CD_ATENDIMENTO", 'NaN') AS "CD_ATENDIMENTO",
            NULLIF(SPLIT_PART(sis."CD_REMESSA", '.', 1), 'NaN') AS "CD_REMESSA",
            sis."DT_REMESSA",
            sis."DT_EXTRACAO"
        FROM {{ source('raw_mv', 'reg_fat')}} sis
        {% if is_incremental() %}
        WHERE sis."CD_REG_FAT"::BIGINT > ( SELECT MAX("CD_REG_FAT") FROM {{ this }} )
        {% endif %}
),
treats
    AS (
        SELECT
            "CD_REG_FAT"::BIGINT,
            "CD_CONVENIO"::BIGINT,
            "CD_ATENDIMENTO"::BIGINT,
            "CD_REMESSA"::BIGINT,
            "DT_REMESSA"::TIMESTAMP,
            "DT_EXTRACAO"::TIMESTAMP
        FROM source_reg_fat
)
SELECT * FROM treats
