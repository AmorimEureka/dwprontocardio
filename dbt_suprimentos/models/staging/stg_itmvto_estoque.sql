{{
    config( materialized = 'incremental',
            unique_key = '"CD_ITMVTO_ESTOQUE"' )
}}

WITH source_itmvto_estoque
    AS (
        SELECT
            NULLIF(SPLIT_PART(sis."CD_ITMVTO_ESTOQUE", '.', 1), 'NaN') AS "CD_ITMVTO_ESTOQUE"
            , NULLIF(sis."CD_MVTO_ESTOQUE", 'NaN') AS "CD_MVTO_ESTOQUE"
            , NULLIF(sis."CD_PRODUTO", 'NaN') AS "CD_PRODUTO"
            , NULLIF(sis."CD_UNI_PRO", 'NaN') AS "CD_UNI_PRO"
            , NULLIF(sis."CD_LOTE", 'NaN') AS "CD_LOTE"
            , NULLIF(SPLIT_PART(sis."CD_ITENT_PRO", '.', 1), 'NaN') AS "CD_ITENT_PRO"
            , NULLIF(SPLIT_PART(sis."CD_FORNECEDOR", '.', 1), 'NaN') AS "CD_FORNECEDOR"
            , NULLIF(sis."CD_ITPRE_MED", 'NaN') AS "CD_ITPRE_MED"
            , sis."DT_VALIDADE"
            , sis."DH_MVTO_ESTOQUE"
            , NULLIF(sis."QT_MOVIMENTACAO", 'NaN') AS "QT_MOVIMENTACAO"
            , NULLIF(sis."VL_UNITARIO", 'NaN') AS "VL_UNITARIO"
            , NULLIF(sis."TP_ESTOQUE", 'NaN') AS "TP_ESTOQUE"
            , sis."DT_EXTRACAO"
        FROM {{ source('raw_mv' , 'itmvto_estoque') }} sis
        {% if is_incremental() %}
        WHERE SPLIT_PART(sis."CD_ITMVTO_ESTOQUE", '.', 1)::BIGINT > ( SELECT MAX("CD_ITMVTO_ESTOQUE") FROM {{this}} )
        {% endif %}
),
treats
    AS (
        SELECT
            "CD_ITMVTO_ESTOQUE"::BIGINT
            , "CD_MVTO_ESTOQUE"::BIGINT
            , "CD_PRODUTO"::BIGINT
            , "CD_UNI_PRO"::BIGINT
            , "CD_LOTE"::VARCHAR(500)
            , "CD_ITENT_PRO"::BIGINT
            , "CD_FORNECEDOR"::BIGINT
            , "CD_ITPRE_MED"::BIGINT
            , "DT_VALIDADE"::DATE
            , "DH_MVTO_ESTOQUE"::TIMESTAMP
            , "QT_MOVIMENTACAO"::NUMERIC(16,6)
            , "VL_UNITARIO"::NUMERIC(14,4)
            , "TP_ESTOQUE"::VARCHAR(1)
            , "DT_EXTRACAO"::TIMESTAMP
        FROM source_itmvto_estoque
    )
SELECT * FROM treats