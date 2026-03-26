{{
    config(
        tags = ['repasse']
    )
}}

WITH source_gru_pro
    AS (
        SELECT
            cd_gru_pro,
            ds_gru_pro
        FROM {{ ref('stg_gru_pro') }}
),
treats
    AS (
        SELECT
            cd_gru_pro,
            ds_gru_pro
        FROM source_gru_pro
)
SELECT * FROM treats