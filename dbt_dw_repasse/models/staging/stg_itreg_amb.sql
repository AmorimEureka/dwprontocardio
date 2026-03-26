{{
    config( materialized = 'incremental',
            unique_key = 'cd_itreg_amb_key',
            merge_update_columns = ['sn_fechada', 'dt_fechamento'],
            tags = ['repasse']
    )
}}

WITH source_itreg_amg
    AS (
        SELECT
            CONCAT(cd_reg_amb, cd_lancamento) AS cd_itreg_amb_key,
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
            hr_lancamento AS dt_itreg_amb,
            sn_fechada,
            sn_repassado,
            sn_pertence_pacote,
            vl_unitario,
            vl_total_conta,
            vl_base_repassado
        FROM {{ source('raw_repasse_mv', 'itreg_amb')}}

),
treats_key
    AS (
        SELECT
            sis.cd_itreg_amb_key,
            sis.cd_pro_fat,
            sis.cd_reg_amb,
            sis.cd_prestador,
            sis.cd_ati_med,
            sis.cd_lancamento,
            sis.cd_gru_fat,
            sis.cd_convenio,
            sis.cd_atendimento,
            sis.dt_producao,
            sis.dt_fechamento,
            sis.dt_itreg_amb,
            sis.sn_fechada,
            sis.sn_repassado,
            sis.sn_pertence_pacote,
            sis.vl_unitario,
            sis.vl_total_conta,
            sis.vl_base_repassado
        FROM source_itreg_amg sis
        {% if is_incremental() %}
        WHERE sis.cd_itreg_amb_key::NUMERIC(20,0) > ( SELECT MAX(cd_itreg_amb_key) FROM {{this}} )
        {% endif %}
),
treats
    AS (
        SELECT
            cd_itreg_amb_key::NUMERIC(20,0),
            cd_pro_fat::VARCHAR(21),
            cd_reg_amb::NUMERIC(10,0),
            cd_prestador::BIGINT,
            cd_ati_med::BIGINT,
            cd_lancamento::NUMERIC(10,0),
            cd_gru_fat::BIGINT,
            cd_convenio::BIGINT,
            cd_atendimento::BIGINT,
            dt_producao::TIMESTAMP,
            dt_fechamento::TIMESTAMP,
            dt_itreg_amb::TIMESTAMP,
            sn_fechada::VARCHAR(1),
            sn_repassado::VARCHAR(1),
            sn_pertence_pacote::VARCHAR(1),
            vl_unitario::NUMERIC(14,4),
            vl_total_conta::NUMERIC(12,2),
            vl_base_repassado::NUMERIC(12,2)
        FROM treats_key
)
SELECT * FROM treats