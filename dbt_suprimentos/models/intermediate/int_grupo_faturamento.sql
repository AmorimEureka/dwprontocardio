

WITH source_gru_fat
    AS (
        SELECT
            "CD_GRU_FAT",
            "DS_GRU_FAT",
            "DT_EXTRACAO"
        FROM {{ ref('stg_gru_fat') }}
),
treats
    AS (
        SELECT
            "CD_GRU_FAT",
            "DS_GRU_FAT",
            "DT_EXTRACAO"
        FROM source_gru_fat
)
SELECT * FROM treats