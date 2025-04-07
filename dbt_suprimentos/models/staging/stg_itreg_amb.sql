{{
    config( materialized = 'incremental',
            unique_key = '"CD_ITREG_AMB_KEY"',
            merge_update_columns = ['"SN_REPASSADO"'] )
}}

WITH source_itreg_amg
    AS (
        SELECT
            NULLIF("CD_PRO_FAT", 'NaN') AS "CD_PRO_FAT",
            NULLIF("CD_REG_AMB", 'NaN') AS "CD_REG_AMB",
            NULLIF(SPLIT_PART("CD_PRESTADOR", '.', 1), 'NaN') AS "CD_PRESTADOR",
            NULLIF("CD_ATI_MED", 'NaN') AS "CD_ATI_MED",
            NULLIF("CD_LANCAMENTO", 'NaN') AS "CD_LANCAMENTO",
            NULLIF("CD_GRU_FAT", 'NaN') AS "CD_GRU_FAT",
            NULLIF("CD_CONVENIO", 'NaN') AS "CD_CONVENIO",
            NULLIF("CD_ATENDIMENTO", 'NaN') AS "CD_ATENDIMENTO",
            "DT_PRODUCAO",
            "DT_FECHAMENTO",
            "DT_ITREG_AMB",
            NULLIF("SN_FECHADA", 'NaN') AS "SN_FECHADA",
            NULLIF("SN_REPASSADO", 'NaN') AS "SN_REPASSADO",
            NULLIF("SN_PERTENCE_PACOTE", 'NaN') AS "SN_PERTENCE_PACOTE",
            NULLIF("VL_UNITARIO", 'NaN') AS "VL_UNITARIO",
            NULLIF("VL_TOTAL_CONTA", 'NaN') AS "VL_TOTAL_CONTA",
            NULLIF("VL_BASE_REPASSADO", 'NaN') AS "VL_BASE_REPASSADO",
            "DT_EXTRACAO"
        FROM {{ source('raw_mv', "itreg_amb")}}

),
treats_key
    AS (
        SELECT
            CONCAT(sis."CD_REG_AMB", sis."CD_LANCAMENTO") AS "CD_ITREG_AMB_KEY",
            sis."CD_PRO_FAT",
            sis."CD_REG_AMB",
            sis."CD_PRESTADOR",
            sis."CD_ATI_MED",
            sis."CD_LANCAMENTO",
            sis."CD_GRU_FAT",
            sis."CD_CONVENIO",
            sis."CD_ATENDIMENTO",
            sis."DT_PRODUCAO",
            sis."DT_FECHAMENTO",
            sis."DT_ITREG_AMB",
            sis."SN_FECHADA",
            sis."SN_REPASSADO",
            sis."SN_PERTENCE_PACOTE",
            sis."VL_UNITARIO",
            sis."VL_TOTAL_CONTA",
            sis."VL_BASE_REPASSADO",
            sis."DT_EXTRACAO"
        FROM source_itreg_amg sis
        {% if is_incremental() %}
        WHERE CONCAT(sis."CD_REG_AMB", sis."CD_LANCAMENTO")::NUMERIC(20,0) > ( SELECT MAX("CD_ITREG_AMB_KEY") FROM {{this}} )
        {% endif %}
),
treats
    AS (
        SELECT
            "CD_ITREG_AMB_KEY"::NUMERIC(20,0),
            "CD_PRO_FAT"::VARCHAR(21),
            "CD_REG_AMB"::NUMERIC(10,0),
            "CD_PRESTADOR"::BIGINT,
            "CD_ATI_MED"::BIGINT,
            "CD_LANCAMENTO"::NUMERIC(10,0),
            "CD_GRU_FAT"::BIGINT,
            "CD_CONVENIO"::BIGINT,
            "CD_ATENDIMENTO"::BIGINT,
            "DT_PRODUCAO"::TIMESTAMP,
            "DT_FECHAMENTO"::TIMESTAMP,
            "DT_ITREG_AMB"::TIMESTAMP,
            "SN_FECHADA"::VARCHAR(1),
            "SN_REPASSADO"::VARCHAR(1),
            "SN_PERTENCE_PACOTE"::VARCHAR(1),
            "VL_UNITARIO"::NUMERIC(14,4),
            "VL_TOTAL_CONTA"::NUMERIC(12,2),
            "VL_BASE_REPASSADO"::NUMERIC(12,2),
            "DT_EXTRACAO"::TIMESTAMP
        FROM treats_key
)
SELECT * FROM treats