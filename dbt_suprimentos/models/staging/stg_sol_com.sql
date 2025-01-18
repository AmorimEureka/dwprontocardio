{{
    config( materialized = 'table' )

}}

WITH source_sol_com 
    AS (
        SELECT
            NULLIF("CD_SOL_COM", 'NaN') AS "CD_SOL_COM"
            , NULLIF("CD_MOT_PED", 'NaN') AS "CD_MOT_PED"
            , NULLIF("CD_SETOR", 'NaN') AS "CD_SETOR"
            , NULLIF("CD_ESTOQUE", 'NaN') AS "CD_ESTOQUE"
            , NULLIF(SPLIT_PART("CD_MOT_CANCEL"::TEXT, '.', 1), 'NaN') AS "CD_MOT_CANCEL"
            , NULLIF("CD_ATENDIME", 'NaN') AS "CD_ATENDIME"
            , NULLIF("CD_USUARIO", 'NaN') AS "CD_SOLICITANTE"
            , NULLIF("NM_SOLICITANTE", 'NaN') AS "NM_SOLICITANTE"
            , "DT_SOL_COM"
            , "DT_CANCELAMENTO"
            , NULLIF("VL_TOTAL", 'NaN') AS "VL_TOTAL"
            , NULLIF("TP_SITUACAO", 'NaN') AS "TP_SITUACAO"
            , NULLIF("TP_SOL_COM", 'NaN') AS "TP_SOL_COM"
            , NULLIF("SN_URGENTE", 'NaN') AS "SN_URGENTE"
            , NULLIF("SN_APROVADA", 'NaN') AS "SN_APROVADA"
            , NULLIF("SN_OPME", 'NaN') AS "SN_OPME"
            , "DT_EXTRACAO"
        FROM {{ source('raw_mv' , 'sol_com') }}
),
treats 
    AS (
        SELECT
            "CD_SOL_COM"::BIGINT
            , "CD_MOT_PED"::BIGINT
            , "CD_SETOR"::BIGINT
            , "CD_ESTOQUE"::BIGINT
            , "CD_MOT_CANCEL"::BIGINT
            , "CD_ATENDIME"::BIGINT
            , "CD_SOLICITANTE"::VARCHAR(50)
            , "NM_SOLICITANTE"::VARCHAR(25)
            , "DT_SOL_COM"::DATE
            , "DT_CANCELAMENTO"::TIMESTAMP
            , "VL_TOTAL"::NUMERIC(12,2)
            , "TP_SITUACAO"::VARCHAR(1)
            , "TP_SOL_COM"::VARCHAR(1)
            , "SN_URGENTE"::VARCHAR(1)
            , "SN_APROVADA"::VARCHAR(1)
            , "SN_OPME"::VARCHAR(1)
            , "DT_EXTRACAO"::TIMESTAMP
        FROM source_sol_com
    )
SELECT * FROM treats