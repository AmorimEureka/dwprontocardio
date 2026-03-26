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
source_item_regra_faturamento
    AS (
        SELECT
            cd_itreg_fat_key,
            cd_pro_fat,
            cd_reg_fat,
            cd_prestador,
            cd_ati_med,
            cd_lancamento,
            cd_gru_fat,
            dt_producao,
            dt_itreg_fat,
            sn_repassado,
            sn_pertence_pacote,
            vl_unitario,
            vl_total_conta,
            vl_base_repassado
        FROM {{ ref('stg_itreg_fat')}}
),
source_regra_faturamento
    AS (
        SELECT
            cd_reg_fat,
            cd_convenio,
            cd_atendimento,
            cd_remessa,
            dt_remessa
        FROM {{ ref('stg_reg_fat')}}
),
treats
    AS (
        SELECT
            irf.cd_itreg_fat_key,
            pf.cd_pro_fat,
            irf.cd_reg_fat,
            irf.cd_prestador,
            irf.cd_ati_med,
            irf.cd_lancamento,
            pf.cd_gru_pro,
            irf.cd_gru_fat,
            rf.cd_convenio,
            rf.cd_atendimento,
            rf.cd_remessa,
            pf.ds_pro_fat,
            rf.dt_remessa,
            irf.dt_producao,
            NULL::DATE AS dt_fechamento,
            irf.dt_itreg_fat,
            NULL::VARCHAR AS sn_fechada,
            irf.sn_repassado,
            irf.sn_pertence_pacote,
            irf.vl_unitario,
            irf.vl_total_conta,
            irf.vl_base_repassado
        FROM source_pro_fat pf
        LEFT JOIN source_item_regra_faturamento irf ON pf.cd_pro_fat = irf.cd_pro_fat
        LEFT JOIN source_regra_faturamento rf ON irf.cd_reg_fat = rf.cd_reg_fat
)
SELECT * FROM treats