{{
    config( materialized = 'incremental',
            unique_key = 'cd_atendimento',
        on_schema_change = 'sync_all_columns',
        tags = ['repasse']
    )
}}

WITH source_atendime
    AS (
        SELECT
            sis.cd_atendimento,
            sis.cd_paciente,
            sis.cd_prestador,
            sis.dt_atendimento,
            sis.tp_atendimento
        FROM {{ source('raw_repasse_mv', 'atendime')}} sis
        {% if is_incremental() %}
        WHERE sis.cd_atendimento::BIGINT > ( SELECT MAX(cd_atendimento) FROM {{this}} )
        {% endif %}
),
treats
    AS (
        SELECT
            cd_atendimento::BIGINT AS cd_atendimento,
            cd_paciente::BIGINT AS cd_paciente,
            cd_prestador::BIGINT AS cd_prestador,
            dt_atendimento::TIMESTAMP AS dt_atendimento,
            tp_atendimento::VARCHAR(1) AS tp_atendimento
        FROM source_atendime
)
SELECT * FROM treats