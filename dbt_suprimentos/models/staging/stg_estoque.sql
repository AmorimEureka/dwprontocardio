{{
    config( materialized = 'incremental',
            unique_key = '"CD_ESTOQUE"' )
}}

WITH source_estoque
    AS (
        SELECT
              NULLIF(sis."CD_ESTOQUE", 'NaN') AS "CD_ESTOQUE"
            , NULLIF(sis."CD_SETOR", 'NaN') AS "CD_SETOR"
            , NULLIF(sis."DS_ESTOQUE", 'NaN') AS "DS_ESTOQUE"
            , NULLIF(sis."TP_ESTOQUE", 'NaN') AS "TP_ESTOQUE"
            , sis."DT_EXTRACAO" AS "DT_EXTRACAO"
        FROM {{ source('raw_mv' , 'estoque') }} sis
        {% if is_incremental() %}
        WHERE sis."CD_ESTOQUE"::BIGINT > ( SELECT MAX("CD_ESTOQUE") FROM {{this}} )
        {% endif %}
),
treats
    AS (
        SELECT
            "CD_ESTOQUE"::BIGINT
            , "CD_SETOR"::BIGINT
            , "DS_ESTOQUE"::VARCHAR(30)
            , "TP_ESTOQUE"::VARCHAR(1)
            , "DT_EXTRACAO"::TIMESTAMP
        FROM source_estoque
)
SELECT * FROM treats