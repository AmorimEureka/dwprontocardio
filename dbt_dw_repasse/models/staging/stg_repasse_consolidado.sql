{{
    config( materialized = 'incremental',
            unique_key = 'cd_repasse_consolidado',
             merge_update_columns = ['cd_reg_fat', 'cd_reg_amb', 'cd_procedimento', 'vl_sp', 'vl_ato'],
             on_schema_change = 'sync_all_columns',
             tags = ['repasse']
    )
}}

WITH source_repasse_consolidado
    AS (
        SELECT
            CONCAT(COALESCE(cd_reg_fat, cd_reg_amb), COALESCE(cd_lanc_fat, cd_lanc_amb)) AS cd_itreg_fat_key,
            cd_repasse_consolidado,
            cd_repasse,
            cd_pro_fat,
            cd_reg_fat,
            cd_reg_amb,
            cd_prestador_repasse,
            cd_ati_med,
            cd_lanc_fat,
            cd_lanc_amb,
            cd_gru_fat,
            cd_gru_pro,
            cd_procedimento,
            dt_lancamento AS dt_repasse_consolidado,
            dt_competencia_fat,
            dt_competencia_rep,
            sn_pertence_pacote,
            vl_sp,
            vl_ato,
            vl_repasse,
            vl_total_conta,
            vl_base_repassado
        FROM {{ source('raw_repasse_mv', 'repasse_consolidado')}}
),
treats_key
    AS (
        SELECT
            sis.cd_itreg_fat_key,
            sis.cd_repasse_consolidado,
            sis.cd_repasse,
            sis.cd_pro_fat,
            sis.cd_reg_fat,
            sis.cd_reg_amb,
            sis.cd_prestador_repasse,
            sis.cd_ati_med,
            sis.cd_lanc_fat,
            sis.cd_lanc_amb,
            sis.cd_gru_fat,
            sis.cd_gru_pro,
            sis.cd_procedimento,
            sis.dt_repasse_consolidado,
            sis.dt_competencia_fat,
            sis.dt_competencia_rep,
            sis.sn_pertence_pacote,
            sis.vl_sp,
            sis.vl_ato,
            sis.vl_repasse,
            sis.vl_total_conta,
            sis.vl_base_repassado
        FROM source_repasse_consolidado sis
        {% if is_incremental() %}
        WHERE sis.cd_repasse_consolidado::BIGINT > ( SELECT MAX(cd_repasse_consolidado) FROM {{this}} )
        {% endif %}
),
treats
    AS (
        SELECT
            cd_itreg_fat_key::NUMERIC(20,0),
            cd_repasse_consolidado::BIGINT,
            cd_repasse::BIGINT,
            cd_pro_fat::VARCHAR(21),
            cd_reg_fat::NUMERIC(10,0),
            cd_reg_amb::NUMERIC(10,0),
            cd_prestador_repasse::BIGINT,
            cd_ati_med::BIGINT,
            cd_lanc_fat::NUMERIC(10,0),
            cd_lanc_amb::NUMERIC(10,0),
            cd_gru_fat::BIGINT,
            cd_gru_pro::BIGINT,
            cd_procedimento::VARCHAR(21),
            dt_repasse_consolidado::TIMESTAMP,
            dt_competencia_fat::TIMESTAMP,
            dt_competencia_rep::TIMESTAMP,
            sn_pertence_pacote::VARCHAR(1),
            vl_sp::NUMERIC(12,2),
            vl_ato::NUMERIC(12,2),
            vl_repasse::NUMERIC(12,2),
            vl_total_conta::NUMERIC(12,2),
            vl_base_repassado::NUMERIC(12,2)
        FROM  treats_key
)
SELECT * FROM treats