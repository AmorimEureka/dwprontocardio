{{
    config( materialized = 'incremental',
            unique_key = '"CD_ESPECIE"' )
}}

WITH source_especie
    AS (
        SELECT
            NULLIF(sis."CD_ESPECIE", 'NaN') AS "CD_ESPECIE"
            , NULLIF(SPLIT_PART(sis."CD_ITEM_RES", '.', 1), 'NaN') AS "CD_ITEM_RES"
            , NULLIF(sis."DS_ESPECIE", 'NaN') AS "DS_ESPECIE"
            , sis."DT_EXTRACAO"
        FROM {{ source('raw_mv' , 'especie') }} sis
        {% if is_incremental() %}
        WHERE sis."CD_ESPECIE"::BIGINT > ( SELECT MAX("CD_ESPECIE") FROM {{this}} )
        {% endif %}
),
treats
    AS (
        SELECT
            "CD_ESPECIE"::BIGINT
            , "CD_ITEM_RES"::BIGINT
            , "DS_ESPECIE"::VARCHAR(240)
            , "DT_EXTRACAO"::TIMESTAMP
        FROM source_especie
    )
SELECT * FROM treats