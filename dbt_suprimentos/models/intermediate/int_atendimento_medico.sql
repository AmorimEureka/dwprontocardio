

WITH source_ati_med
    AS (
        SELECT
            "CD_ATI_MED",
            "DS_ATI_MED",
            "DT_EXTRACAO"
        FROM {{ ref('stg_ati_med') }}
),
treats
    AS (
        SELECT
            "CD_ATI_MED",
            "DS_ATI_MED",
            "DT_EXTRACAO"
        FROM source_ati_med
)
SELECT * FROM treats