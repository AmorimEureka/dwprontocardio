{{
    config( materialized = 'incremental',
            unique_key = '"CD_SETOR"' )
}}

WITH source_setor
    AS (
        SELECT
            NULLIF(sis."CD_SETOR", 'NaN') AS "CD_SETOR"
            , NULLIF(sis."CD_FATOR", 'NaN') AS "CD_FATOR"
            , NULLIF(sis."CD_GRUPO_DE_CUSTO", 'NaN') AS "CD_GRUPO_DE_CUSTO"
            , NULLIF(SPLIT_PART(sis."CD_SETOR_CUSTO", '.', 1), 'NaN') AS "CD_SETOR_CUSTO"
            , NULLIF(sis."NM_SETOR", 'NaN') AS "NM_SETOR"
            , NULLIF(sis."SN_ATIVO", 'NaN') AS "SN_ATIVO"
            , sis."DT_EXTRACAO"
        FROM {{ source('raw_mv' , 'setor') }} sis
        {% if is_incremental() %}
        WHERE sis."CD_SETOR"::BIGINT > ( SELECT MAX("CD_SETOR") FROM {{this}} )
        {% endif %}
),
treats
    AS (
        SELECT
            "CD_SETOR"::BIGINT
            , "CD_FATOR"::BIGINT
            , "CD_GRUPO_DE_CUSTO"::BIGINT
            , "CD_SETOR_CUSTO"::BIGINT
            , "NM_SETOR"::VARCHAR(70)
            , "SN_ATIVO"::VARCHAR(1)
            , "DT_EXTRACAO"::TIMESTAMP
        FROM source_setor
    )
SELECT * FROM treats