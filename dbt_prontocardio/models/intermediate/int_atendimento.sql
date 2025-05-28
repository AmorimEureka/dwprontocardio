

WITH source_atendime
    AS (
        SELECT
            "CD_ATENDIMENTO",
            "DT_ATENDIMENTO",
            "CD_PACIENTE",
            "TP_ATENDIMENTO",
            "DT_EXTRACAO"
        FROM {{ ref('stg_atendime') }}
),
treats
    AS (
        SELECT
            "CD_ATENDIMENTO",
            "DT_ATENDIMENTO",
            "CD_PACIENTE",
            "TP_ATENDIMENTO",
            "DT_EXTRACAO"
        FROM source_atendime
)
SELECT * FROM treats