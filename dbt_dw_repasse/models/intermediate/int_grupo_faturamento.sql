{{
    config(
        tags = ['repasse']
    )
}}

WITH source_gru_fat
    AS (
        SELECT
            cd_gru_fat,
            ds_gru_fat
        FROM {{ ref('stg_gru_fat') }}
),
treats
    AS (
        SELECT
            cd_gru_fat,
            ds_gru_fat
        FROM source_gru_fat
)
SELECT * FROM treats