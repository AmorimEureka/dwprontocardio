{{
    config( materialized = 'incremental',
            unique_key = '"CD_ITENT_PRO"' )
}}

WITH source_itent_pro
    AS (
        SELECT
            NULLIF(SPLIT_PART(sis."CD_ITENT_PRO"::TEXT, '.', 1), 'NaN') AS "CD_ITENT_PRO"
            , NULLIF(sis."CD_ENT_PRO", 'NaN') AS "CD_ENT_PRO"
            , NULLIF(sis."CD_PRODUTO", 'NaN') AS "CD_PRODUTO"
            , NULLIF(sis."CD_UNI_PRO", 'NaN') AS "CD_UNI_PRO"
            , NULLIF(SPLIT_PART(sis."CD_ATENDIMENTO"::TEXT, '.', 1), 'NaN') AS "CD_ATENDIMENTO"
            , NULLIF(sis."CD_CUSTO_MEDIO", 'NaN') AS "CD_CUSTO_MEDIO"
            , NULLIF(sis."CD_PRODUTO_FORNECEDOR", 'NaN') AS "CD_PRODUTO_FORNECEDOR"
            , sis."DT_GRAVACAO"
            , NULLIF(sis."QT_ENTRADA", 'NaN') AS "QT_ENTRADA"
            , NULLIF(sis."QT_DEVOLVIDA", 'NaN') AS "QT_DEVOLVIDA"
            , NULLIF(sis."QT_ATENDIDA", 'NaN') AS "QT_ATENDIDA"
            , NULLIF(sis."VL_UNITARIO", 'NaN') AS "VL_UNITARIO"
            , NULLIF(sis."VL_CUSTO_REAL", 'NaN') AS "VL_CUSTO_REAL"
            , NULLIF(sis."VL_TOTAL_CUSTO_REAL", 'NaN') AS "VL_TOTAL_CUSTO_REAL"
            , NULLIF(sis."VL_TOTAL", 'NaN') AS "VL_TOTAL"
            , sis."DT_EXTRACAO"
        FROM {{ source('raw_mv' , 'itent_pro') }} sis
        {% if is_incremental() %}
        WHERE NULLIF(SPLIT_PART(sis."CD_ITENT_PRO"::TEXT, '.', 1), 'NaN')::BIGINT > ( SELECT MAX("CD_ITENT_PRO") FROM {{this}} )
        {% endif %}
),
treats
    AS (
        SELECT
            "CD_ITENT_PRO"::BIGINT
            , "CD_ENT_PRO"::BIGINT
            , "CD_PRODUTO"::BIGINT
            , "CD_UNI_PRO"::BIGINT
            , "CD_ATENDIMENTO"::BIGINT
            , "CD_CUSTO_MEDIO"::BIGINT
            , "CD_PRODUTO_FORNECEDOR"::BIGINT
            , "DT_GRAVACAO"::TIMESTAMP
            , "QT_ENTRADA"::NUMERIC(11,3)
            , "QT_DEVOLVIDA"::NUMERIC(11,3)
            , "QT_ATENDIDA"::NUMERIC(11,3)
            , "VL_UNITARIO"::NUMERIC(12,2)
            , "VL_CUSTO_REAL"::NUMERIC(12,2)
            , "VL_TOTAL_CUSTO_REAL"::NUMERIC(12,2)
            , "VL_TOTAL"::NUMERIC(12,2)
            , "DT_EXTRACAO"::TIMESTAMP
        FROM source_itent_pro
    )
SELECT * FROM treats