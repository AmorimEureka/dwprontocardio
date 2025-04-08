

WITH source_prestador
    AS (
        SELECT
            "CD_PRESTADOR",
            "NM_PRESTADOR",
            "DT_EXTRACAO"
        FROM {{ ref('stg_prestador') }}
),
treats
    AS (
        SELECT
            "CD_PRESTADOR",
            "NM_PRESTADOR",
            "DT_EXTRACAO"
        FROM source_prestador
)
SELECT * FROM treats