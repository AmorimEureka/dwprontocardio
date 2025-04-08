{{
    config( materialized = 'incremental',
            unique_key = '"CD_IT_REPASSE"' )
}}


WITH source_it_repasse
    AS(
        SELECT
            NULLIF(sis."CD_IT_REPASSE", 'NaN') AS "CD_IT_REPASSE"
            , NULLIF(sis."CD_REPASSE", 'NaN') AS "CD_REPASSE"
            , NULLIF(SPLIT_PART(sis."CD_REG_AMB"::TEXT, '.', 1), 'NaN') AS "CD_REG_AMB"
            , NULLIF(SPLIT_PART(sis."CD_LANCAMENTO_AMB"::TEXT, '.', 1), 'NaN') AS "CD_LANCAMENTO_AMB"
            , NULLIF(SPLIT_PART(sis."CD_REG_FAT"::TEXT, '.', 1), 'NaN') AS "CD_REG_FAT"
            , NULLIF(SPLIT_PART(sis."CD_LANCAMENTO_FAT"::TEXT, '.', 1), 'NaN') AS "CD_LANCAMENTO_FAT"
            , NULLIF(sis."CD_ATI_MED", 'NaN') AS "CD_ATI_MED"
            , NULLIF(sis."CD_PRESTADOR_REPASSE", 'NaN') AS "CD_PRESTADOR_REPASSE"
            , NULLIF(sis."VL_REPASSE", 'NaN') AS "VL_REPASSE"
            , sis."DT_EXTRACAO"
        FROM {{ source('raw_mv', 'it_repasse') }} sis
        {% if is_incremental() %}
        WHERE sis."CD_IT_REPASSE"::NUMERIC(10,0) > ( SELECT MAX("CD_IT_REPASSE") FROM {{this}} )
        {% endif %}
),
treats
    AS(
        SELECT
            "CD_IT_REPASSE"::NUMERIC(10,0)
            , "CD_REPASSE"::BIGINT
            , "CD_REG_AMB"::BIGINT
            , "CD_LANCAMENTO_AMB"::BIGINT
            , "CD_REG_FAT"::BIGINT
            , "CD_LANCAMENTO_FAT"::BIGINT
            , "CD_ATI_MED"::BIGINT
            , "CD_PRESTADOR_REPASSE"::BIGINT
            , "VL_REPASSE"::NUMERIC(12,2)
            , "DT_EXTRACAO"::TIMESTAMP
        FROM source_it_repasse
)
SELECT * FROM treats