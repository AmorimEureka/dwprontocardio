

WITH source_gru_pro
    AS (
        SELECT
            "CD_GRU_PRO",
            "DS_GRU_PRO",
            "DT_EXTRACAO"
        FROM {{ ref('stg_gru_pro') }}
),
treats
    AS (
        SELECT
            "CD_GRU_PRO",
            "DS_GRU_PRO",
            "DT_EXTRACAO"
        FROM source_gru_pro
)
SELECT * FROM treats