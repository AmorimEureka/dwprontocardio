{{
    config( materialized = 'incremental',
            unique_key = '"CD_ITREG_FAT_KEY"',
             merge_update_columns = ['"SN_REPASSADO"'] )
}}

WITH source_itreg_fat
    AS (
        SELECT
            NULLIF("CD_PRO_FAT", 'NaN') AS "CD_PRO_FAT",
            NULLIF("CD_REG_FAT", 'NaN') AS "CD_REG_FAT",
            NULLIF(SPLIT_PART("CD_PRESTADOR", '.', 1), 'NaN') AS "CD_PRESTADOR",
            NULLIF("CD_ATI_MED", 'NaN') AS "CD_ATI_MED",
            NULLIF("CD_LANCAMENTO", 'NaN') AS "CD_LANCAMENTO",
            NULLIF("CD_GRU_FAT", 'NaN') AS "CD_GRU_FAT",
            NULLIF("CD_PROCEDIMENTO", 'NaN') AS "CD_PROCEDIMENTO",
            "DT_PRODUCAO",
            "DT_ITREG_FAT",
            NULLIF("SN_REPASSADO", 'NaN') AS "SN_REPASSADO",
            NULLIF("SN_PERTENCE_PACOTE", 'NaN') AS "SN_PERTENCE_PACOTE",
            NULLIF("VL_UNITARIO", 'NaN') AS "VL_UNITARIO",
            NULLIF("VL_TOTAL_CONTA", 'NaN') AS "VL_TOTAL_CONTA",
            NULLIF("VL_BASE_REPASSADO", 'NaN') AS "VL_BASE_REPASSADO",
            "DT_EXTRACAO"
        FROM {{ source('raw_mv', 'itreg_fat')}}
),
treats_key
    AS (
        SELECT
            CONCAT(sis."CD_REG_FAT", sis."CD_LANCAMENTO") AS "CD_ITREG_FAT_KEY",
            sis."CD_PRO_FAT",
            sis."CD_REG_FAT",
            sis."CD_PRESTADOR",
            sis."CD_ATI_MED",
            sis."CD_LANCAMENTO",
            sis."CD_GRU_FAT",
            sis."CD_PROCEDIMENTO",
            sis."DT_PRODUCAO",
            sis."DT_ITREG_FAT",
            sis."SN_REPASSADO",
            sis."SN_PERTENCE_PACOTE",
            sis."VL_UNITARIO",
            sis."VL_TOTAL_CONTA",
            sis."VL_BASE_REPASSADO",
            sis."DT_EXTRACAO"
        FROM source_itreg_fat sis
        {% if is_incremental() %}
        WHERE (CONCAT(sis."CD_REG_FAT", sis."CD_LANCAMENTO")::NUMERIC(20,0) > ( SELECT MAX("CD_ITREG_FAT_KEY") FROM {{this}} ))
        {% endif %}
),
treats
    AS (
        SELECT
            "CD_ITREG_FAT_KEY"::NUMERIC(20,0),
            "CD_PRO_FAT"::VARCHAR(21),
            "CD_REG_FAT"::NUMERIC(10,0),
            "CD_PRESTADOR"::BIGINT,
            "CD_ATI_MED"::BIGINT,
            "CD_LANCAMENTO"::NUMERIC(10,0),
            "CD_GRU_FAT"::BIGINT,
            "CD_PROCEDIMENTO"::VARCHAR(21),
            "DT_PRODUCAO"::TIMESTAMP,
            "DT_ITREG_FAT"::TIMESTAMP,
            "SN_REPASSADO"::VARCHAR(1),
            "SN_PERTENCE_PACOTE"::VARCHAR(1),
            "VL_UNITARIO"::NUMERIC(14,4),
            "VL_TOTAL_CONTA"::NUMERIC(12,2),
            "VL_BASE_REPASSADO"::NUMERIC(12,2),
            "DT_EXTRACAO"::TIMESTAMP
        FROM  treats_key
)
SELECT * FROM treats