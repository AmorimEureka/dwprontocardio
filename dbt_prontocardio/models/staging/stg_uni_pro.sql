{{
    config( materialized = 'incremental',
            unique_key = '"CD_UNI_PRO"' )
}}

WITH source_uni_pro
    AS (
        SELECT
            NULLIF(sis."CD_UNI_PRO", 'NaN') AS "CD_UNI_PRO"
            , NULLIF(sis."CD_UNIDADE", 'NaN') AS "CD_UNIDADE"
            , NULLIF(sis."CD_PRODUTO", 'NaN') AS "CD_PRODUTO"
            , NULLIF(sis."VL_FATOR", 'NaN') AS "VL_FATOR"
            , NULLIF(sis."DS_UNIDADE", 'NaN') AS "DS_UNIDADE"
            , NULLIF(sis."TP_RELATORIOS", 'NaN') AS "TP_RELATORIOS"
            , NULLIF(sis."SN_ATIVO", 'NaN') AS "SN_ATIVO"
            , sis."DT_EXTRACAO"
        FROM {{ source('raw_mv' , 'uni_pro') }} sis
        {% if is_incremental() %}
        WHERE sis."CD_UNI_PRO"::BIGINT > ( SELECT MAX("CD_UNI_PRO") FROM {{this}} )
        {% endif %}
),
treats
    AS (
        SELECT
            "CD_UNI_PRO"::BIGINT
            , "CD_UNIDADE"::VARCHAR(6)
            , "CD_PRODUTO"::BIGINT
            , "VL_FATOR"::NUMERIC(20,8)
            , "DS_UNIDADE"::VARCHAR(30)
            , "TP_RELATORIOS"::VARCHAR(1)
            , "SN_ATIVO"::VARCHAR(1)
            , "DT_EXTRACAO"::TIMESTAMP
        FROM source_uni_pro
    )
SELECT * FROM treats