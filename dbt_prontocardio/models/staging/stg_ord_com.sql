{{
    config( materialized = 'incremental',
            incremental_strategy = 'merge',
            unique_key = '"CD_ORD_COM"',
            merge_update_columns = ['"TP_SITUACAO"', '"CD_MOT_CANCEL"', '"DT_CANCELAMENTO"'] )
}}

WITH source_ord_com
    AS (
        SELECT
            NULLIF(sis."CD_ORD_COM", 'NaN') AS "CD_ORD_COM"
            , NULLIF(sis."CD_ESTOQUE", 'NaN') AS "CD_ESTOQUE"
            , NULLIF(sis."CD_FORNECEDOR", 'NaN') AS "CD_FORNECEDOR"
            , NULLIF(SPLIT_PART(sis."CD_SOL_COM"::TEXT, '.', 1), 'NaN') AS "CD_SOL_COM"
            , NULLIF(SPLIT_PART(sis."CD_MOT_CANCEL"::TEXT, '.', 1), 'NaN') AS "CD_MOT_CANCEL"
            , sis."CD_USUARIO_CRIADOR_OC"
            , sis."CD_ULTIMO_USU_ALT_OC"
            , sis."DT_ORD_COM"
            , sis."DT_PREV_ENTREGA"
            , sis."DT_CANCELAMENTO"
            , sis."DT_AUTORIZACAO"
            , sis."DT_ULTIMA_ALTERACAO_OC"
            , NULLIF(sis."TP_SITUACAO", 'NaN') AS "TP_SITUACAO"
            , NULLIF(sis."TP_ORD_COM", 'NaN') AS "TP_ORD_COM"
            , NULLIF(sis."SN_AUTORIZADO", 'NaN') AS "SN_AUTORIZADO"
            , sis."DT_EXTRACAO"
        FROM {{ source('raw_mv' , 'ord_com') }} sis
        {% if is_incremental() %}
        WHERE (
                sis."DT_ORD_COM" >= (current_date - interval '30 day')
                or
                sis."CD_ORD_COM"::BIGINT > ( SELECT MAX("CD_ORD_COM") FROM {{this}} )
                )
        {% endif %}
),
treats
    AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY "CD_ORD_COM" ORDER BY "DT_EXTRACAO" DESC) as rn
        FROM (
            SELECT
                "CD_ORD_COM"::BIGINT
                , "CD_ESTOQUE"::BIGINT
                , "CD_FORNECEDOR"::BIGINT
                , "CD_SOL_COM"::BIGINT
                , "CD_MOT_CANCEL"::BIGINT
                , "CD_USUARIO_CRIADOR_OC"::VARCHAR(30)
                , "CD_ULTIMO_USU_ALT_OC"::VARCHAR(30)
                , "DT_ORD_COM"::DATE
                , "DT_PREV_ENTREGA"::DATE
                , "DT_CANCELAMENTO"::TIMESTAMP
                , "DT_AUTORIZACAO"::TIMESTAMP
                , "DT_ULTIMA_ALTERACAO_OC"::DATE
                , "TP_SITUACAO"::VARCHAR(1)
                , "TP_ORD_COM"::VARCHAR(1)
                , "SN_AUTORIZADO"::VARCHAR(1)
                , "DT_EXTRACAO"::TIMESTAMP
            FROM source_ord_com
        ) base
    )
SELECT * FROM treats WHERE rn = 1
