{{
    config( materialized = 'incremental',
            unique_key = 'cd_itreg_fat_key',
             merge_update_columns = ['dt_producao', 'dt_lancamento', 'sn_repassado',
             'vl_sp', 'vl_ato'],
             tags = ['repasse']
    )
}}

WITH source_itreg_fat
    AS (
        SELECT
            CONCAT(itf.cd_reg_fat, itf.cd_lancamento) AS cd_itreg_fat_key,
            itf.cd_pro_fat,
            itf.cd_reg_fat,
            itf.cd_prestador,
            itf.cd_ati_med,
            itf.cd_lancamento,
            itf.cd_gru_fat,
            itf.cd_procedimento,
            itf.dt_producao,
            itf.dt_lancamento AS dt_itreg_fat,
            itf.sn_repassado,
            itf.sn_pertence_pacote,
            itf.vl_unitario,
            itf.vl_sp,
            itf.vl_ato,
            itf.vl_total_conta,
            itf.vl_base_repassado
        FROM {{ source('raw_repasse_mv', 'itreg_fat')}} itf
),
treats_key
    AS (
        SELECT
            sis.cd_itreg_fat_key,
            sis.cd_pro_fat,
            sis.cd_reg_fat,
            sis.cd_prestador,
            sis.cd_ati_med,
            sis.cd_lancamento,
            sis.cd_gru_fat,
            sis.cd_procedimento,
            sis.dt_producao,
            sis.dt_itreg_fat,
            sis.sn_repassado,
            sis.sn_pertence_pacote,
            sis.vl_unitario,
            sis.vl_sp,
            sis.vl_ato,
            sis.vl_total_conta,
            sis.vl_base_repassado
        FROM source_itreg_fat sis
        {% if is_incremental() %}
        WHERE sis.cd_itreg_fat_key::NUMERIC(20,0) > ( SELECT MAX(cd_itreg_fat_key) FROM {{this}} )
        {% endif %}
),
treats
    AS (
        SELECT
            cd_itreg_fat_key::NUMERIC(20,0),
            cd_pro_fat::VARCHAR(21),
            cd_reg_fat::NUMERIC(10,0),
            cd_prestador::BIGINT,
            cd_ati_med::BIGINT,
            cd_lancamento::NUMERIC(10,0),
            cd_gru_fat::BIGINT,
            cd_procedimento::VARCHAR(21),
            dt_producao::TIMESTAMP,
            dt_itreg_fat::TIMESTAMP,
            sn_repassado::VARCHAR(1),
            sn_pertence_pacote::VARCHAR(1),
            vl_unitario::NUMERIC(14,4),
            vl_sp::NUMERIC(12,2),
            vl_ato::NUMERIC(12,2),
            vl_total_conta::NUMERIC(12,2),
            vl_base_repassado::NUMERIC(12,2)
        FROM  treats_key
)
SELECT * FROM treats