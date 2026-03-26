{{
    config(
        tags = ['repasse']
    )
}}

WITH source_atendime
    AS (
        SELECT
            cd_atendimento,
            dt_atendimento,
            cd_paciente,
            tp_atendimento
        FROM {{ ref('stg_atendime') }}
),
treats
    AS (
        SELECT
            cd_atendimento,
            dt_atendimento,
            cd_paciente,
            tp_atendimento
        FROM source_atendime
)
SELECT * FROM treats