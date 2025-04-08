

WITH source_pro_fat
    AS (
        SELECT
            "CD_PRO_FAT",
            "CD_GRU_PRO",
            "DS_PRO_FAT"
        FROM {{ ref('stg_pro_fat')}}
),
source_item_regra_faturamento
    AS (
        SELECT
            "CD_ITREG_FAT_KEY",
            "CD_PRO_FAT",
            "CD_REG_FAT",
            "CD_PRESTADOR",
            "CD_ATI_MED",
            "CD_LANCAMENTO",
            "CD_GRU_FAT",
            "DT_PRODUCAO",
            "DT_ITREG_FAT",
            "SN_REPASSADO",
            "SN_PERTENCE_PACOTE",
            "VL_UNITARIO",
            "VL_TOTAL_CONTA",
            "VL_BASE_REPASSADO"
        FROM {{ ref('stg_itreg_fat')}}
),
source_regra_faturamento
    AS (
        SELECT
            "CD_REG_FAT",
            "CD_CONVENIO",
            "CD_ATENDIMENTO",
            "CD_REMESSA",
            "DT_REMESSA"
        FROM {{ ref('stg_reg_fat')}}
),
treats
    AS (
        SELECT
            irf."CD_ITREG_FAT_KEY",
            pf."CD_PRO_FAT",
            irf."CD_REG_FAT",
            irf."CD_PRESTADOR",
            irf."CD_ATI_MED",
            irf."CD_LANCAMENTO",
            pf."CD_GRU_PRO",
            irf."CD_GRU_FAT",
            rf."CD_CONVENIO",
            rf."CD_ATENDIMENTO",
            rf."CD_REMESSA",
            pf."DS_PRO_FAT",
            rf."DT_REMESSA",
            irf."DT_PRODUCAO",
            NULL::DATE AS "DT_FECHAMENTO",
            irf."DT_ITREG_FAT",
            NULL::VARCHAR AS "SN_FECHADA",
            irf."SN_REPASSADO",
            irf."SN_PERTENCE_PACOTE",
            irf."VL_UNITARIO",
            irf."VL_TOTAL_CONTA",
            irf."VL_BASE_REPASSADO"
        FROM source_pro_fat pf
        LEFT JOIN source_item_regra_faturamento irf ON pf."CD_PRO_FAT" = irf."CD_PRO_FAT"
        LEFT JOIN source_regra_faturamento rf ON irf."CD_REG_FAT" = rf."CD_REG_FAT"
)
SELECT * FROM treats