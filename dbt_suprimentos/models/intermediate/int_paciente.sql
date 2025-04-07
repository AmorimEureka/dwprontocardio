

WITH source_paciente
    AS (
        SELECT
            "CD_PACIENTE",
            "NM_PACIENTE",
            "DT_EXTRACAO"
        FROM {{ ref('stg_paciente') }}
),
treats
    AS (
        SELECT
            "CD_PACIENTE",
            "NM_PACIENTE",
            "DT_EXTRACAO"
        FROM source_paciente
)
SELECT * FROM treats