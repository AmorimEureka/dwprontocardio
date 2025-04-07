{{
    config( materialized = 'incremental',
            unique_key = '"CD_FORNECEDOR"' )
}}

WITH source_fornecedor
    AS (
        SELECT
            NULLIF(sis."CD_FORNECEDOR", 'NaN') AS "CD_FORNECEDOR"
            , NULLIF(sis."NM_FORNECEDOR", 'NaN') AS "NM_FORNECEDOR"
            , NULLIF(sis."NM_FANTASIA", 'NaN') AS "NM_FANTASIA"
            , sis."DT_INCLUSAO"
            , NULLIF(sis."NR_CGC_CPF", 'NaN') AS "NR_CGC_CPF"
            , NULLIF(sis."TP_FORNECEDOR", 'NaN') AS "TP_FORNECEDOR"
            , sis."DT_EXTRACAO"
        FROM {{ source('raw_mv' , 'fornecedor') }} sis
        {% if is_incremental() %}
        WHERE sis."CD_FORNECEDOR"::BIGINT > ( SELECT MAX("CD_FORNECEDOR") FROM {{this}} )
        {% endif %}

),
treats
    AS (
        SELECT
            "CD_FORNECEDOR"::BIGINT
            , "NM_FORNECEDOR"::VARCHAR(250)
            , "NM_FANTASIA"::VARCHAR(100)
            , "DT_INCLUSAO"::DATE
            , "NR_CGC_CPF"::NUMERIC(14,0)
            , "TP_FORNECEDOR"::VARCHAR(1)
            , "DT_EXTRACAO"::TIMESTAMP
        FROM source_fornecedor
)
SELECT * FROM treats