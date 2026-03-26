{{
    config(
        tags = ['repasse']
    )
}}

WITH source_ati_med
    AS (
        SELECT
            cd_ati_med,
            ds_ati_med
        FROM {{ ref('stg_ati_med') }}
),
treats
    AS (
        SELECT
            cd_ati_med,
            ds_ati_med
        FROM source_ati_med
)
SELECT * FROM treats