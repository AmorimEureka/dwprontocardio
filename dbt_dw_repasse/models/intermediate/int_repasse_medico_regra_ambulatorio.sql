{{
    config(
        tags = ['repasse']
    )
}}

WITH source_pro_fat
    AS (
        SELECT
            cd_pro_fat,
            cd_gru_pro,
            ds_pro_fat
        FROM {{ ref('stg_pro_fat')}}
),
source_item_regra_ambulatorio
    AS (
        SELECT
            cd_itreg_amb_key,
            cd_pro_fat,
            cd_reg_amb,
            cd_prestador,
            cd_ati_med,
            cd_lancamento,
            cd_gru_fat,
            cd_convenio,
            cd_atendimento,
            dt_producao,
            dt_fechamento,
            dt_itreg_amb,
            sn_fechada,
            sn_repassado,
            sn_pertence_pacote,
            vl_unitario,
            vl_total_conta,
            vl_base_repassado
        FROM {{ ref('stg_itreg_amb')}}
),
source_regra_ambulatorio
    AS (
        SELECT
            cd_reg_amb,
            cd_remessa,
            dt_remessa
        FROM {{ ref('stg_reg_amb')}}
),
treats
    AS (
        SELECT
            ia.cd_itreg_amb_key,
            pf.cd_pro_fat,
            ia.cd_reg_amb,
            ia.cd_prestador,
            ia.cd_ati_med,
            ia.cd_lancamento,
            pf.cd_gru_pro,
            ia.cd_gru_fat,
            ia.cd_convenio,
            ia.cd_atendimento,
            ra.cd_remessa,
            pf.ds_pro_fat,
            ra.dt_remessa,
            ia.dt_producao,
            ia.dt_fechamento,
            ia.dt_itreg_amb,
            ia.sn_fechada,
            ia.sn_repassado,
            ia.sn_pertence_pacote,
            ia.vl_unitario,
            ia.vl_total_conta,
            ia.vl_base_repassado
        FROM source_pro_fat pf
        LEFT JOIN source_item_regra_ambulatorio ia ON pf.cd_pro_fat = ia.cd_pro_fat
        LEFT JOIN source_regra_ambulatorio ra ON ia.cd_reg_amb = ra.cd_reg_amb
)
SELECT * FROM treats