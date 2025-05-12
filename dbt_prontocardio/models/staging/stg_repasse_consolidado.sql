{{
    config( materialized = 'incremental',
            unique_key = '"CD_ITREG_FAT_KEY"',
             merge_update_columns = ['"CD_REG_FAT"','"CD_PROCEDIMENTO"', '"VL_SP"', '"VL_ATO"']
             )
}}

WITH source_repasse_consolidado
    AS (
        SELECT
            NULLIF("CD_PRO_FAT", 'NaN') AS "CD_PRO_FAT",
            NULLIF(SPLIT_PART("CD_REG_FAT", '.', 1), 'NaN') AS "CD_REG_FAT",
            NULLIF("CD_PRESTADOR_REPASSE", 'NaN') AS "CD_PRESTADOR_REPASSE",
            NULLIF("CD_ATI_MED", 'NaN') AS "CD_ATI_MED",
            NULLIF(SPLIT_PART("CD_LANC_FAT", '.', 1), 'NaN') AS "CD_LANC_FAT",
            NULLIF(SPLIT_PART("CD_GRU_FAT", '.', 1), 'NaN') AS "CD_GRU_FAT",
            NULLIF(SPLIT_PART("CD_GRU_PRO", '.', 1), 'NaN') AS "CD_GRU_PRO",
            NULLIF("CD_PROCEDIMENTO", 'NaN') AS "CD_PROCEDIMENTO",
            "DT_REPASSE_CONSOLIDADO",
            "DT_COMPETENCIA_FAT",
            "DT_COMPETENCIA_REP",
            "SN_PERTENCE_PACOTE",
            COALESCE(NULLIF("VL_SP", 'NaN'), '0') AS "VL_SP",
            COALESCE(NULLIF("VL_ATO", 'NaN'), '0') AS "VL_ATO",
            COALESCE(NULLIF("VL_REPASSE", 'NaN'), '0') AS "VL_REPASSE",
            COALESCE(NULLIF("VL_TOTAL_CONTA", 'NaN'), '0') AS "VL_TOTAL_CONTA",
            COALESCE(NULLIF("VL_BASE_REPASSADO", 'NaN'), '0') AS "VL_BASE_REPASSADO",
            "DT_EXTRACAO"
        FROM {{ source('raw_mv', 'repasse_consolidado')}}
        WHERE NULLIF(SPLIT_PART("CD_REG_FAT", '.', 1), 'NaN') IS NOT NULL
),
treats_key
    AS (
        SELECT
            CONCAT(sis."CD_REG_FAT", sis."CD_LANC_FAT") AS "CD_ITREG_FAT_KEY",
            sis."CD_PRO_FAT",
            sis."CD_REG_FAT",
            sis."CD_PRESTADOR_REPASSE",
            sis."CD_ATI_MED",
            sis."CD_LANC_FAT",
            sis."CD_GRU_FAT",
            sis."CD_GRU_PRO",
            sis."CD_PROCEDIMENTO",
            sis."DT_REPASSE_CONSOLIDADO",
            sis."DT_COMPETENCIA_FAT",
            sis."DT_COMPETENCIA_REP",
            sis."SN_PERTENCE_PACOTE",
            sis."VL_SP",
            sis."VL_ATO",
            sis."VL_REPASSE",
            sis."VL_TOTAL_CONTA",
            sis."VL_BASE_REPASSADO",
            sis."DT_EXTRACAO"
        FROM source_repasse_consolidado sis
        {% if is_incremental() %}
        WHERE (CONCAT(sis."CD_REG_FAT", sis."CD_LANC_FAT")::NUMERIC(20,0) > ( SELECT MAX("CD_ITREG_FAT_KEY") FROM {{this}} ))
        {% endif %}
),
treats
    AS (
        SELECT
            "CD_ITREG_FAT_KEY"::NUMERIC(20,0),
            "CD_PRO_FAT"::VARCHAR(21),
            "CD_REG_FAT"::NUMERIC(10,0),
            "CD_PRESTADOR_REPASSE"::BIGINT,
            "CD_ATI_MED"::BIGINT,
            "CD_LANC_FAT"::NUMERIC(10,0),
            "CD_GRU_FAT"::BIGINT,
            "CD_GRU_PRO"::BIGINT,
            "CD_PROCEDIMENTO"::VARCHAR(21),
            "DT_REPASSE_CONSOLIDADO"::TIMESTAMP,
            "DT_COMPETENCIA_FAT"::TIMESTAMP,
            "DT_COMPETENCIA_REP"::TIMESTAMP,
            "SN_PERTENCE_PACOTE"::VARCHAR(1),
            "VL_SP"::NUMERIC(12,2),
            "VL_ATO"::NUMERIC(12,2),
            "VL_REPASSE"::NUMERIC(12,2),
            "VL_TOTAL_CONTA"::NUMERIC(12,2),
            "VL_BASE_REPASSADO"::NUMERIC(12,2),
            "DT_EXTRACAO"::TIMESTAMP
        FROM  treats_key
)
SELECT * FROM treats