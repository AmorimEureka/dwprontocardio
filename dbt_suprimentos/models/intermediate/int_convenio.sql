

WITH source_convenio
    AS (
        SELECT
            "CD_CONVENIO",
            "NM_CONVENIO",
            "DT_EXTRACAO"
        FROM {{ ref('stg_convenio') }}
),
treats
    AS (
        SELECT
            "CD_CONVENIO",
            "NM_CONVENIO",
            "DT_EXTRACAO"
        FROM source_convenio
)
SELECT * FROM treats