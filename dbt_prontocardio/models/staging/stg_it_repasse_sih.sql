{{
    config( materialized = 'incremental',
            unique_key = '"CD_IT_REPASSE_SIH"' )
}}

WITH source_it_repasse_sih
    AS (
        SELECT
            "CD_IT_REPASSE_SIH"
            , "CD_REPASSE"
            , "CD_REG_FAT"
            , "CD_LANCAMENTO"
            , "CD_ATI_MED"
            , "CD_PRESTADOR_REPASSE"
            , "VL_REPASSE"
            , "DT_EXTRACAO"
        FROM {{ source('raw_mv', 'it_repasse_sih')}}
),
treats_key
    AS (
        SELECT
            sis."CD_IT_REPASSE_SIH"
            , sis."CD_REPASSE"
            , sis."CD_REG_FAT"
            , sis."CD_LANCAMENTO"
            , sis."CD_ATI_MED"
            , sis."CD_PRESTADOR_REPASSE"
            , sis."VL_REPASSE"
            , sis."DT_EXTRACAO"
        FROM source_it_repasse_sih sis
        {% if is_incremental() %}
        WHERE sis."CD_IT_REPASSE_SIH"::NUMERIC(10,0) > ( SELECT MAX("CD_IT_REPASSE_SIH") FROM {{this}} )
        {% endif %}
),
treats
    AS (
        SELECT
            "CD_IT_REPASSE_SIH"::NUMERIC(10,0)
            , "CD_REPASSE"::BIGINT
            , "CD_REG_FAT"::BIGINT
            , "CD_LANCAMENTO"::BIGINT
            , "CD_ATI_MED"::BIGINT
            , "CD_PRESTADOR_REPASSE"::BIGINT
            , "VL_REPASSE"::NUMERIC(12,2)
            , "DT_EXTRACAO"::TIMESTAMP
        FROM treats_key
)
SELECT * FROM treats