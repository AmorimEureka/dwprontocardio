{{

    config( materialized = 'incremental',
            unique_key = 'sk_repasse_medico',
            post_hook = [
                "DELETE FROM {{ this }} AS tgt USING {{ ref('stg_reg_amb') }} AS src WHERE tgt.tp_regra = 'AMBULATORIO' AND tgt.cd_regra = src.cd_reg_amb AND tgt.cd_remessa IS NULL AND src.cd_remessa IS NOT NULL",
                "DELETE FROM {{ this }} AS tgt USING {{ ref('stg_reg_fat') }} AS src WHERE tgt.tp_regra = 'HOSPITALAR' AND tgt.cd_regra = src.cd_reg_fat AND tgt.cd_remessa IS NULL AND src.cd_remessa IS NOT NULL"
            ],
            on_schema_change = 'sync_all_columns',
            tags = ['repasse']
    )
}}

WITH source_int_repasses_medicos
    AS (
        SELECT
            *
        FROM {{ ref('int_repasses_medicos') }} sis
        {% if is_incremental() %}
        WHERE GREATEST(
                COALESCE(sis.dt_competencia, TIMESTAMP '1900-01-01'),
                COALESCE(sis.dt_repasse, TIMESTAMP '1900-01-01'),
                COALESCE(sis.dt_producao, TIMESTAMP '1900-01-01'),
                COALESCE(sis.dt_itregra, TIMESTAMP '1900-01-01'),
                COALESCE(sis.dt_fechamento, TIMESTAMP '1900-01-01')
            )
            >= CURRENT_DATE
                - make_interval(days => {{ var('f_repasses_medicos_lookback_days', 90) }})
        {% endif %}
),
source_keyed
    AS (
        SELECT
            CONCAT_WS(
                ':',
                CASE WHEN cd_repasse IS NULL THEN 'REGRA' ELSE 'REPASSE' END,
                COALESCE(cd_repasse::TEXT, ''),
                COALESCE(cd_prestador_repasse::TEXT, ''),
                COALESCE(cd_ati_med::TEXT, ''),
                COALESCE(tp_regra, ''),
                COALESCE(cd_itreg_key::TEXT, ''),
                COALESCE(cd_atendimento::TEXT, ''),
                COALESCE(cd_regra::TEXT, ''),
                COALESCE(cd_lancamento::TEXT, '')
            ) AS sk_repasse_medico,
            sis.*
        FROM source_int_repasses_medicos sis
),
source_incremental
    AS (
        SELECT
            sis.*
        FROM source_keyed sis
        {% if is_incremental() %}
        LEFT JOIN {{ this }} tgt
            ON tgt.sk_repasse_medico = sis.sk_repasse_medico
        WHERE tgt.sk_repasse_medico IS NULL
            OR tgt.cd_repasse IS DISTINCT FROM sis.cd_repasse
            OR tgt.cd_remessa IS DISTINCT FROM sis.cd_remessa
            OR tgt.cd_convenio IS DISTINCT FROM sis.cd_convenio
            OR tgt.dt_competencia IS DISTINCT FROM sis.dt_competencia
            OR tgt.sn_fechada IS DISTINCT FROM sis.sn_fechada
            OR tgt.dt_fechamento IS DISTINCT FROM sis.dt_fechamento
            OR tgt.sn_repassado IS DISTINCT FROM sis.sn_repassado
            OR tgt.vl_repasse IS DISTINCT FROM sis.vl_repasse
        {% endif %}
),
mrt_repasses_medicos
    AS (
        SELECT
            *
        FROM source_incremental
)
SELECT
    *
FROM mrt_repasses_medicos
