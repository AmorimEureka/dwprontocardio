

WITH source_pro_fat
    AS (
        SELECT
            "CD_PRO_FAT",
            "CD_GRU_PRO",
            "DS_PRO_FAT"
        FROM {{ ref('stg_pro_fat')}}
),
source_item_regra_ambulatorio
    AS (
        SELECT
            "CD_ITREG_AMB_KEY",
            "CD_PRO_FAT",
            "CD_REG_AMB",
            "CD_PRESTADOR",
            "CD_ATI_MED",
            "CD_LANCAMENTO",
            "CD_GRU_FAT",
            "CD_CONVENIO",
            "CD_ATENDIMENTO",
            "DT_PRODUCAO",
            "DT_FECHAMENTO",
            "DT_ITREG_AMB",
            "SN_FECHADA",
            "SN_REPASSADO",
            "SN_PERTENCE_PACOTE",
            "VL_UNITARIO",
            "VL_TOTAL_CONTA",
            "VL_BASE_REPASSADO"
        FROM {{ ref('stg_itreg_amb')}}
),
source_regra_ambulatorio
    AS (
        SELECT
            "CD_REG_AMB",
            "CD_REMESSA",
            "DT_REMESSA"
        FROM {{ ref('stg_reg_amb')}}
),
treats
    AS (
        SELECT
            ia."CD_ITREG_AMB_KEY",
            pf."CD_PRO_FAT",
            ia."CD_REG_AMB",
            ia."CD_PRESTADOR",
            ia."CD_ATI_MED",
            ia."CD_LANCAMENTO",
            pf."CD_GRU_PRO",
            ia."CD_GRU_FAT",
            ia."CD_CONVENIO",
            ia."CD_ATENDIMENTO",
            ra."CD_REMESSA",
            pf."DS_PRO_FAT",
            ra."DT_REMESSA",
            ia."DT_PRODUCAO",
            ia."DT_FECHAMENTO",
            ia."DT_ITREG_AMB",
            ia."SN_FECHADA",
            ia."SN_REPASSADO",
            ia."SN_PERTENCE_PACOTE",
            ia."VL_UNITARIO",
            ia."VL_TOTAL_CONTA",
            ia."VL_BASE_REPASSADO"
        FROM source_pro_fat pf
        LEFT JOIN source_item_regra_ambulatorio ia ON pf."CD_PRO_FAT" = ia."CD_PRO_FAT"
        LEFT JOIN source_regra_ambulatorio ra ON ia."CD_REG_AMB" = ra."CD_REG_AMB"
)
SELECT * FROM treats