{{
    config( materialized = 'table' )

}}

WITH source_ord_com 
    AS (
        SELECT
            NULLIF("CD_ORD_COM", 'NaN') AS "CD_ORD_COM"
            , NULLIF("CD_ESTOQUE", 'NaN') AS "CD_ESTOQUE"
            , NULLIF("CD_FORNECEDOR", 'NaN') AS "CD_FORNECEDOR"
            , NULLIF(SPLIT_PART("CD_SOL_COM"::TEXT, '.', 1), 'NaN') AS "CD_SOL_COM"
            , NULLIF(SPLIT_PART("CD_MOT_CANCEL"::TEXT, '.', 1), 'NaN') AS "CD_MOT_CANCEL"
            , "CD_USUARIO_CRIADOR_OC"
            , "CD_ULTIMO_USU_ALT_OC"
            , "DT_ORD_COM"
            , "DT_CANCELAMENTO"
            , "DT_AUTORIZACAO"
            , "DT_ULTIMA_ALTERACAO_OC"
            , NULLIF("TP_SITUACAO", 'NaN') AS "TP_SITUACAO"
            , NULLIF("TP_ORD_COM", 'NaN') AS "TP_ORD_COM"
            , NULLIF("SN_AUTORIZADO", 'NaN') AS "SN_AUTORIZADO"
            , "DT_EXTRACAO"
        FROM {{ source('raw_mv' , 'ord_com') }}
),
treats
    AS (
        SELECT
            "CD_ORD_COM"::BIGINT
            , "CD_ESTOQUE"::BIGINT
            , "CD_FORNECEDOR"::BIGINT
            , "CD_SOL_COM"::BIGINT
            , "CD_MOT_CANCEL"::BIGINT
            , "CD_USUARIO_CRIADOR_OC"::VARCHAR(30)
            , "CD_ULTIMO_USU_ALT_OC"::VARCHAR(30)
            , "DT_ORD_COM"::DATE
            , "DT_CANCELAMENTO"::TIMESTAMP
            , "DT_AUTORIZACAO"::TIMESTAMP
            , "DT_ULTIMA_ALTERACAO_OC"::DATE
            , "TP_SITUACAO"::VARCHAR(1)
            , "TP_ORD_COM"::VARCHAR(1)
            , "SN_AUTORIZADO"::VARCHAR(1)
            , "DT_EXTRACAO"::TIMESTAMP
        FROM source_ord_com
)
SELECT * FROM treats