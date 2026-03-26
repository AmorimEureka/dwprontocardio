{{
    config(
        tags = ['repasse']
    )
}}

WITH source_repasse
    AS (
        SELECT
            cd_repasse,
            dt_competencia,
            dt_repasse,
            tp_repasse
        FROM {{ ref('stg_repasse')}}
),
source_item_repasse
    AS (
        SELECT
            cd_repasse,
            cd_reg_amb,
            cd_lancamento_amb,
            cd_reg_fat,
            cd_lancamento_fat,
            cd_ati_med,
            cd_prestador_repasse,
            vl_repasse
        FROM {{ ref('stg_it_repasse')}}
),
source_item_repasse_sih
    AS (
        SELECT
            cd_repasse,
            NULL::NUMERIC(10,0) AS cd_reg_amb,
            NULL::NUMERIC(10,0) AS cd_lancamento_amb,
            cd_reg_fat,
            cd_lancamento AS cd_lancamento_fat,
            cd_ati_med,
            cd_prestador_repasse,
            vl_repasse
        FROM {{ ref('stg_it_repasse_sih')}}
),
treats_repasse
    AS (
        SELECT
            ir.cd_repasse,
            ir.cd_reg_amb,
            ir.cd_lancamento_amb,
            ir.cd_reg_fat,
            ir.cd_lancamento_fat,
            ir.cd_ati_med,
            ir.cd_prestador_repasse,
            ir.vl_repasse,
            r.dt_competencia,
            r.dt_repasse,
            r.tp_repasse
        FROM source_item_repasse ir
        LEFT JOIN source_repasse r ON ir.cd_repasse = r.cd_repasse
),
treats_repasse_sih
    AS (
        SELECT
            sih.cd_repasse,
            sih.cd_reg_amb,
            sih.cd_lancamento_amb,
            sih.cd_reg_fat,
            sih.cd_lancamento_fat,
            sih.cd_ati_med,
            sih.cd_prestador_repasse,
            sih.vl_repasse,
            rish.dt_competencia,
            rish.dt_repasse,
            rish.tp_repasse
        FROM source_item_repasse_sih sih
        LEFT JOIN source_repasse rish ON sih.cd_repasse = rish.cd_repasse
),
treats_repasse_consolidado
    AS (
        SELECT
            *
        FROM treats_repasse

        UNION ALL

        SELECT
            *
        FROM treats_repasse_sih
),
source_repasse_prestador
    AS (
        SELECT
            rp.cd_repasse,
            rp.cd_prestador_repasse,
            rp.cd_repasse AS cd_itreg_key,
            rp.dt_competencia,
            rp.dt_repasse,
            rp.tp_repasse,
            rp.vl_repasse
        FROM {{ ref('stg_repasse_prestador')}} rp
        WHERE rp.tp_repasse = 'M'
),
source_atendimento
    AS (
        SELECT
            cd_atendimento,
            cd_paciente,
            cd_prestador,
            tp_atendimento
        FROM {{ ref('stg_atendime')}}
),
source_pro_fat
    AS (
        SELECT
            cd_pro_fat,
            cd_gru_pro,
            ds_pro_fat
        FROM {{ ref('stg_pro_fat')}}
),
source_regra_ambulatorio
    AS (
        SELECT
            cd_reg_amb,
            cd_remessa,
            dt_remessa
        FROM {{ ref('stg_reg_amb')}}
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
treats_regra_ambulatorio
    AS (
        SELECT
            ira.cd_itreg_amb_key,
            pf.cd_pro_fat,
            ira.cd_reg_amb,
            ira.cd_prestador,
            sa.cd_paciente,
            ira.cd_ati_med,
            ira.cd_lancamento,
            pf.cd_gru_pro,
            ira.cd_gru_fat,
            ira.cd_convenio,
            ira.cd_atendimento,
            ra.cd_remessa,
            pf.ds_pro_fat,
            ra.dt_remessa,
            ira.dt_producao,
            ira.dt_fechamento,
            ira.dt_itreg_amb,
            ira.sn_fechada,
            ira.sn_repassado,
            ira.sn_pertence_pacote,
            ira.vl_unitario,
            ira.vl_total_conta,
            ira.vl_base_repassado
        FROM source_item_regra_ambulatorio ira
        LEFT JOIN source_pro_fat pf ON ira.cd_pro_fat = pf.cd_pro_fat
        LEFT JOIN source_regra_ambulatorio ra ON ira.cd_reg_amb = ra.cd_reg_amb
        LEFT JOIN source_atendimento sa ON ira.cd_atendimento = sa.cd_atendimento
        WHERE pf.cd_gru_pro <> 28
),
source_regra_faturamento
    AS (
        SELECT
            cd_reg_fat,
            cd_convenio,
            cd_atendimento,
            cd_remessa,
            dt_remessa,
            dt_fechamento,
            sn_fechada
        FROM {{ ref('stg_reg_fat')}}
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
            cd_procedimento,
            -- dt_fechamento,
            dt_producao,
            dt_itreg_fat,
            -- sn_fechada,
            sn_repassado,
            sn_pertence_pacote,
            vl_unitario,
            vl_sp,
            vl_ato,
            vl_total_conta,
            vl_base_repassado
        FROM {{ ref('stg_itreg_fat')}}
),
source_repasse_consolidado
    AS (
        SELECT
            cd_itreg_fat_key,
            cd_repasse,
            cd_pro_fat,
            cd_reg_fat,
            cd_prestador_repasse,
            cd_ati_med,
            cd_lanc_fat,
            cd_gru_fat,
            cd_gru_pro,
            cd_procedimento,
            dt_repasse_consolidado,
            dt_competencia_fat,
            dt_competencia_rep,
            sn_pertence_pacote,
            vl_sp,
            vl_ato,
            vl_repasse,
            vl_total_conta,
            vl_base_repassado
        FROM {{ref('stg_repasse_consolidado')}}
),
treats_regra_faturamento
    AS (
        SELECT
            irf.cd_itreg_fat_key,
            pf.cd_pro_fat,
            irf.cd_procedimento,
            irf.cd_reg_fat,
            irf.cd_prestador,
            sa.cd_paciente,
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
            rf.dt_fechamento,
            irf.dt_itreg_fat,
            rf.sn_fechada,
            irf.sn_repassado,
            irf.sn_pertence_pacote,
            irf.vl_unitario,
            irf.vl_total_conta,
            irf.vl_base_repassado
        FROM source_item_regra_faturamento irf
        LEFT JOIN source_pro_fat pf ON irf.cd_pro_fat = pf.cd_pro_fat
        LEFT JOIN source_regra_faturamento rf ON irf.cd_reg_fat = rf.cd_reg_fat
        LEFT JOIN source_atendimento sa ON rf.cd_atendimento = sa.cd_atendimento
        WHERE irf.cd_reg_fat IS NOT NULL AND pf.cd_gru_pro <> 28
),
treats_regra_ambulatorio_sem_remessa
    AS (
        SELECT
            NULL AS cd_repasse,
            tra.cd_reg_amb AS cd_regra,
            tra.cd_lancamento,
            tra.cd_itreg_amb_key AS cd_itreg_key,
            tra.cd_pro_fat,
            tra.cd_gru_pro,
            tra.cd_gru_fat,
            sa.cd_atendimento,
            tra.cd_remessa,
            tra.cd_convenio,
            NULL AS cd_ati_med,
            tra.cd_prestador AS cd_prestador_repasse,
            sa.cd_paciente,
            tra.dt_itreg_amb AS dt_itregra,
            NULL AS dt_competencia,
            NULL AS dt_repasse,
            tra.dt_producao,
            tra.dt_fechamento,
            NULL AS tp_repasse,
            'AMBULATORIO'::VARCHAR(12) AS tp_regra,
            tra.sn_fechada,
            tra.sn_repassado,
            tra.sn_pertence_pacote,
            NULL AS vl_repasse,
            tra.vl_unitario,
            tra.vl_total_conta,
            tra.vl_base_repassado
        FROM source_atendimento sa
        LEFT JOIN treats_regra_ambulatorio tra ON sa.cd_atendimento = tra.cd_atendimento
        WHERE sa.tp_atendimento IN('A', 'E')
),
treats_regra_faturamento_sem_remessa
    AS (
        SELECT
            NULL AS cd_repasse,
            trf.cd_reg_fat AS cd_regra,
            trf.cd_lancamento,
            trf.cd_itreg_fat_key AS cd_itreg_key,
            trf.cd_pro_fat,
            trf.cd_gru_pro,
            trf.cd_gru_fat,
            sa.cd_atendimento,
            trf.cd_remessa,
            trf.cd_convenio,
            NULL AS cd_ati_med,
            trf.cd_prestador AS cd_prestador_repasse,
            sa.cd_paciente,
            trf.dt_itreg_fat AS dt_itregra,
            NULL AS dt_competencia,
            NULL AS dt_repasse,
            trf.dt_producao,
            trf.dt_fechamento,
            NULL AS tp_repasse,
            'HOSPITALAR'::VARCHAR(12) AS tp_regra,
            trf.sn_fechada,
            trf.sn_repassado,
            trf.sn_pertence_pacote,
            NULL AS vl_repasse,
            trf.vl_unitario,
            trf.vl_total_conta,
            trf.vl_base_repassado
        FROM source_atendimento sa
        LEFT JOIN treats_regra_faturamento trf ON sa.cd_atendimento = trf.cd_atendimento
        WHERE sa.tp_atendimento IN('I', 'U')
),
treats_regra_sem_remessa_consolidado
    AS (
        SELECT * FROM treats_regra_ambulatorio_sem_remessa tras WHERE tras.cd_remessa IS NULL
        UNION ALL
        SELECT * FROM treats_regra_faturamento_sem_remessa trfs WHERE trfs.cd_remessa IS NULL
),
treats_repasse_regra_ambulatorio
    AS (
        SELECT
            trc.cd_repasse,
            tra.cd_reg_amb AS cd_regra,
            tra.cd_lancamento,
            tra.cd_itreg_amb_key AS cd_itreg_key,
            tra.cd_pro_fat,
            tra.cd_gru_pro,
            tra.cd_gru_fat,
            tra.cd_atendimento,
            tra.cd_remessa,
            tra.cd_convenio,
            trc.cd_ati_med,
            trc.cd_prestador_repasse,
            tra.cd_paciente,
            tra.dt_itreg_amb AS dt_itregra,
            trc.dt_competencia,
            trc.dt_repasse,
            tra.dt_producao,
            tra.dt_fechamento,
            trc.tp_repasse,
            'AMBULATORIO'::VARCHAR(12) AS tp_regra,
            tra.sn_fechada,
            tra.sn_repassado,
            tra.sn_pertence_pacote,
            trc.vl_repasse,
            tra.vl_unitario,
            tra.vl_total_conta,
            tra.vl_base_repassado
        FROM treats_repasse_consolidado trc
        INNER JOIN treats_regra_ambulatorio tra ON trc.cd_reg_amb = tra.cd_reg_amb AND trc.cd_lancamento_amb = tra.cd_lancamento
),
treats_repasse_regra_faturamento
    AS (
        SELECT
            trc.cd_repasse,
            trf.cd_reg_fat AS cd_regra,
            trf.cd_lancamento,
            trf.cd_itreg_fat_key AS cd_itreg_key,
            CASE WHEN trf.cd_pro_fat = 'X0000000' THEN
                trf.cd_procedimento
            ELSE trf.cd_pro_fat
            END AS cd_pro_fat,
            trf.cd_gru_pro,
            trf.cd_gru_fat,
            trf.cd_atendimento,
            trf.cd_remessa,
            trf.cd_convenio,
            trc.cd_ati_med,
            trc.cd_prestador_repasse,
            trf.cd_paciente,
            trf.dt_itreg_fat AS dt_itregra,
            trc.dt_competencia,
            trc.dt_repasse,
            trf.dt_producao,
            trf.dt_fechamento,
            trc.tp_repasse,
            'HOSPITALAR'::VARCHAR(12) AS tp_regra,
            trf.sn_fechada,
            trf.sn_repassado,
            trf.sn_pertence_pacote,
            CASE WHEN trf.cd_pro_fat = 'X0000000' THEN
                rc.vl_base_repassado
            ELSE rc.vl_repasse
            END AS vl_repasse,
            trf.vl_unitario,
            CASE WHEN trf.cd_pro_fat = 'X0000000' THEN
                rc.vl_sp + rc.vl_ato
            ELSE rc.vl_total_conta
            END AS vl_total_conta,
            trf.vl_base_repassado
        FROM treats_repasse_consolidado trc
        INNER JOIN treats_regra_faturamento trf ON trc.cd_reg_fat = trf.cd_reg_fat AND trc.cd_lancamento_fat = trf.cd_lancamento
        LEFT JOIN source_repasse_consolidado rc ON trc.cd_reg_fat = rc.cd_reg_fat AND trc.cd_lancamento_fat = rc.cd_lanc_fat
                --   AND trc.cd_prestador_repasse = rc.cd_prestador_repasse
                --   AND COALESCE(trc.cd_ati_med, 1) = COALESCE(rc.cd_ati_med, 1)
),
treats_repasse_manual
    AS (
        SELECT
            rp.cd_repasse,
            NULL AS cd_regra,
            NULL AS cd_lancamento,
            rp.cd_itreg_key,
            NULL AS cd_pro_fat,
            NULL AS cd_gru_pro,
            NULL AS cd_gru_fat,
            NULL AS cd_atendimento,
            0 AS cd_remessa,
            NULL AS cd_convenio,
            rp.cd_repasse AS cd_ati_med,
            rp.cd_prestador_repasse,
            NULL AS cd_paciente,
            NULL AS dt_itregra,
            rp.dt_competencia,
            rp.dt_repasse,
            NULL AS dt_producao,
            NULL AS dt_fechamento,
            rp.tp_repasse,
            'LANC. MANUAL'::VARCHAR(12) AS tp_regra,
            NULL AS sn_fechada,
            NULL AS sn_repassado,
            NULL AS sn_pertence_pacote,
            rp.vl_repasse,
            NULL AS vl_unitario,
            NULL AS vl_total_conta,
            NULL AS vl_base_repassado
        FROM source_repasse_prestador rp
),
treats_unioned
    AS (
        SELECT
            cd_repasse::BIGINT AS cd_repasse,
            cd_regra::NUMERIC(10,0) AS cd_regra,
            cd_lancamento::NUMERIC(10,0) AS cd_lancamento,
            cd_itreg_key::NUMERIC(20,0) AS cd_itreg_key,
            cd_pro_fat::VARCHAR(21) AS cd_pro_fat,
            cd_gru_pro::BIGINT AS cd_gru_pro,
            cd_gru_fat::BIGINT AS cd_gru_fat,
            cd_atendimento::BIGINT AS cd_atendimento,
            cd_remessa::BIGINT AS cd_remessa,
            cd_convenio::BIGINT AS cd_convenio,
            cd_ati_med::BIGINT AS cd_ati_med,
            cd_prestador_repasse::BIGINT AS cd_prestador_repasse,
            cd_paciente::BIGINT AS cd_paciente,
            dt_itregra::TIMESTAMP AS dt_itregra,
            dt_competencia::TIMESTAMP AS dt_competencia,
            dt_repasse::TIMESTAMP AS dt_repasse,
            dt_producao::TIMESTAMP AS dt_producao,
            dt_fechamento::TIMESTAMP AS dt_fechamento,
            tp_repasse::VARCHAR(1) AS tp_repasse,
            tp_regra::VARCHAR(12) AS tp_regra,
            sn_fechada::VARCHAR(1) AS sn_fechada,
            sn_repassado::VARCHAR(1) AS sn_repassado,
            sn_pertence_pacote::VARCHAR(1) AS sn_pertence_pacote,
            vl_repasse::NUMERIC(12, 2) AS vl_repasse,
            vl_unitario::NUMERIC(14,4) AS vl_unitario,
            vl_total_conta::NUMERIC(12,2) AS vl_total_conta,
            vl_base_repassado::NUMERIC(12,2) AS vl_base_repassado
        FROM treats_repasse_regra_ambulatorio

        UNION ALL

        SELECT
            cd_repasse::BIGINT AS cd_repasse,
            cd_regra::NUMERIC(10,0) AS cd_regra,
            cd_lancamento::NUMERIC(10,0) AS cd_lancamento,
            cd_itreg_key::NUMERIC(20,0) AS cd_itreg_key,
            cd_pro_fat::VARCHAR(21) AS cd_pro_fat,
            cd_gru_pro::BIGINT AS cd_gru_pro,
            cd_gru_fat::BIGINT AS cd_gru_fat,
            cd_atendimento::BIGINT AS cd_atendimento,
            cd_remessa::BIGINT AS cd_remessa,
            cd_convenio::BIGINT AS cd_convenio,
            cd_ati_med::BIGINT AS cd_ati_med,
            cd_prestador_repasse::BIGINT AS cd_prestador_repasse,
            cd_paciente::BIGINT AS cd_paciente,
            dt_itregra::TIMESTAMP AS dt_itregra,
            dt_competencia::TIMESTAMP AS dt_competencia,
            dt_repasse::TIMESTAMP AS dt_repasse,
            dt_producao::TIMESTAMP AS dt_producao,
            dt_fechamento::TIMESTAMP AS dt_fechamento,
            tp_repasse::VARCHAR(1) AS tp_repasse,
            tp_regra::VARCHAR(12) AS tp_regra,
            sn_fechada::VARCHAR(1) AS sn_fechada,
            sn_repassado::VARCHAR(1) AS sn_repassado,
            sn_pertence_pacote::VARCHAR(1) AS sn_pertence_pacote,
            vl_repasse::NUMERIC(12, 2) AS vl_repasse,
            vl_unitario::NUMERIC(14,4) AS vl_unitario,
            vl_total_conta::NUMERIC(12,2) AS vl_total_conta,
            vl_base_repassado::NUMERIC(12,2) AS vl_base_repassado
        FROM treats_repasse_regra_faturamento

        UNION ALL

        SELECT
            cd_repasse::BIGINT AS cd_repasse,
            cd_regra::NUMERIC(10,0) AS cd_regra,
            cd_lancamento::NUMERIC(10,0) AS cd_lancamento,
            cd_itreg_key::NUMERIC(20,0) AS cd_itreg_key,
            cd_pro_fat::VARCHAR(21) AS cd_pro_fat,
            cd_gru_pro::BIGINT AS cd_gru_pro,
            cd_gru_fat::BIGINT AS cd_gru_fat,
            cd_atendimento::BIGINT AS cd_atendimento,
            cd_remessa::BIGINT AS cd_remessa,
            cd_convenio::BIGINT AS cd_convenio,
            cd_ati_med::BIGINT AS cd_ati_med,
            cd_prestador_repasse::BIGINT AS cd_prestador_repasse,
            cd_paciente::BIGINT AS cd_paciente,
            dt_itregra::TIMESTAMP AS dt_itregra,
            dt_competencia::TIMESTAMP AS dt_competencia,
            dt_repasse::TIMESTAMP AS dt_repasse,
            dt_producao::TIMESTAMP AS dt_producao,
            dt_fechamento::TIMESTAMP AS dt_fechamento,
            tp_repasse::VARCHAR(1) AS tp_repasse,
            tp_regra::VARCHAR(12) AS tp_regra,
            sn_fechada::VARCHAR(1) AS sn_fechada,
            sn_repassado::VARCHAR(1) AS sn_repassado,
            sn_pertence_pacote::VARCHAR(1) AS sn_pertence_pacote,
            vl_repasse::NUMERIC(12, 2) AS vl_repasse,
            vl_unitario::NUMERIC(14,4) AS vl_unitario,
            vl_total_conta::NUMERIC(12,2) AS vl_total_conta,
            vl_base_repassado::NUMERIC(12,2) AS vl_base_repassado
        FROM treats_repasse_manual

        UNION ALL

        SELECT
            cd_repasse::BIGINT AS cd_repasse,
            cd_regra::NUMERIC(10,0) AS cd_regra,
            cd_lancamento::NUMERIC(10,0) AS cd_lancamento,
            cd_itreg_key::NUMERIC(20,0) AS cd_itreg_key,
            cd_pro_fat::VARCHAR(21) AS cd_pro_fat,
            cd_gru_pro::BIGINT AS cd_gru_pro,
            cd_gru_fat::BIGINT AS cd_gru_fat,
            cd_atendimento::BIGINT AS cd_atendimento,
            cd_remessa::BIGINT AS cd_remessa,
            cd_convenio::BIGINT AS cd_convenio,
            cd_ati_med::BIGINT AS cd_ati_med,
            cd_prestador_repasse::BIGINT AS cd_prestador_repasse,
            cd_paciente::BIGINT AS cd_paciente,
            dt_itregra::TIMESTAMP AS dt_itregra,
            dt_competencia::TIMESTAMP AS dt_competencia,
            dt_repasse::TIMESTAMP AS dt_repasse,
            dt_producao::TIMESTAMP AS dt_producao,
            dt_fechamento::TIMESTAMP AS dt_fechamento,
            tp_repasse::VARCHAR(1) AS tp_repasse,
            tp_regra::VARCHAR(12) AS tp_regra,
            sn_fechada::VARCHAR(1) AS sn_fechada,
            sn_repassado::VARCHAR(1) AS sn_repassado,
            sn_pertence_pacote::VARCHAR(1) AS sn_pertence_pacote,
            vl_repasse::NUMERIC(12, 2) AS vl_repasse,
            vl_unitario::NUMERIC(14,4) AS vl_unitario,
            vl_total_conta::NUMERIC(12,2) AS vl_total_conta,
            vl_base_repassado::NUMERIC(12,2) AS vl_base_repassado
        FROM treats_regra_sem_remessa_consolidado
),
treats
    AS (
        SELECT * FROM treats_unioned
)
SELECT * FROM treats