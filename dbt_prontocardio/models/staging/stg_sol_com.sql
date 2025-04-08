{{
    config( materialized = 'incremental',
            unique_key = '"CD_SOL_COM"' )

}}

WITH source_sol_com
    AS (
        SELECT
            NULLIF(sis."CD_SOL_COM", 'NaN') AS "CD_SOL_COM"
            , NULLIF(sis."CD_MOT_PED", 'NaN') AS "CD_MOT_PED"
            , NULLIF(sis."CD_SETOR", 'NaN') AS "CD_SETOR"
            , NULLIF(sis."CD_ESTOQUE", 'NaN') AS "CD_ESTOQUE"
            , NULLIF(SPLIT_PART(sis."CD_MOT_CANCEL"::TEXT, '.', 1), 'NaN') AS "CD_MOT_CANCEL"
            , NULLIF(sis."CD_ATENDIME", 'NaN') AS "CD_ATENDIME"
            , NULLIF(sis."CD_USUARIO", 'NaN') AS "CD_SOLICITANTE"
            , NULLIF(sis."NM_SOLICITANTE", 'NaN') AS "NM_SOLICITANTE"
            , sis."DT_SOL_COM"
            , sis."DT_CANCELAMENTO"
            , NULLIF(sis."VL_TOTAL", 'NaN') AS "VL_TOTAL"
            , NULLIF(sis."TP_SITUACAO", 'NaN') AS "TP_SITUACAO"
            , NULLIF(sis."TP_SOL_COM", 'NaN') AS "TP_SOL_COM"
            , NULLIF(sis."SN_URGENTE", 'NaN') AS "SN_URGENTE"
            , NULLIF(sis."SN_APROVADA", 'NaN') AS "SN_APROVADA"
            , NULLIF(sis."SN_OPME", 'NaN') AS "SN_OPME"
            , sis."DT_EXTRACAO"
        FROM {{ source('raw_mv' , 'sol_com') }} sis
        {% if is_incremental() %}
        WHERE sis."CD_SOL_COM"::BIGINT > ( SELECT MAX("CD_SOL_COM") FROM {{this}} )
        {% endif %}
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