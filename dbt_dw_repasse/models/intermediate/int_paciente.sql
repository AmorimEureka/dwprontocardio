{{
    config(
        tags = ['repasse']
    )
}}

WITH source_paciente
    AS (
        SELECT
            cd_paciente,
            nm_paciente
        FROM {{ ref('stg_paciente') }}
),
treats
    AS (
        SELECT
            cd_paciente,
            nm_paciente
        FROM source_paciente
)
SELECT * FROM treats