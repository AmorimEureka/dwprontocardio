{{
    config( materialized = 'incremental',
            unique_key = '"CD_ENT_PRO"' )

}}

WITH source_ent_pro
    AS (
        SELECT
            NULLIF(sis."CD_ENT_PRO", 'NaN') AS "CD_ENT_PRO"
            , NULLIF(sis."CD_TIP_ENT", 'NaN') AS "CD_TIP_ENT"
            , NULLIF(sis."CD_ESTOQUE", 'NaN') AS "CD_ESTOQUE"
            , NULLIF(sis."CD_FORNECEDOR", 'NaN') AS "CD_FORNECEDOR"
            , NULLIF(SPLIT_PART(sis."CD_ORD_COM"::TEXT,'.', 1), 'NaN') AS "CD_ORD_COM"
            , NULLIF(sis."CD_USUARIO_RECEBIMENTO", 'NaN') AS "CD_USUARIO_RECEBIMENTO"
            , NULLIF(sis."CD_ATENDIMENTO", 'NaN') AS "CD_ATENDIMENTO"
            , sis."DT_EMISSAO"
            , sis."DT_ENTRADA"
            , sis."DT_RECEBIMENTO"
            , sis."HR_ENTRADA"
            , NULLIF(sis."VL_TOTAL", 'NaN') AS "VL_TOTAL"
            , NULLIF(sis."NR_DOCUMENTO", 'NaN') AS "NR_DOCUMENTO"
            , NULLIF(sis."NR_CHAVE_ACESSO", 'NaN') AS "NR_CHAVE_ACESSO"
            , NULLIF(sis."SN_AUTORIZADO", 'NaN') AS "SN_AUTORIZADO"
            , sis."DT_EXTRACAO"
        FROM {{ source('raw_mv' , 'ent_pro') }} sis
        {% if is_incremental() %}
        WHERE sis."CD_ENT_PRO"::BIGINT > ( SELECT MAX("CD_ENT_PRO") FROM {{this}} )
        {% endif %}
),
treats
    AS (
        SELECT
            "CD_ENT_PRO"::BIGINT
            , "CD_TIP_ENT"::BIGINT
            , "CD_ESTOQUE"::BIGINT
            , "CD_FORNECEDOR"::BIGINT
            , "CD_ORD_COM"::BIGINT
            , "CD_USUARIO_RECEBIMENTO"::VARCHAR(50)
            , "CD_ATENDIMENTO"::BIGINT
            , "DT_EMISSAO"::DATE
            , "DT_ENTRADA"::DATE
            , "DT_RECEBIMENTO"::TIMESTAMP
            , "HR_ENTRADA"::TIMESTAMP
            , "VL_TOTAL"::NUMERIC(12,2)
            , "NR_DOCUMENTO"::VARCHAR(15)
            , "NR_CHAVE_ACESSO"::VARCHAR(44)
            , "SN_AUTORIZADO"::VARCHAR(1)
            , "DT_EXTRACAO"::TIMESTAMP
        FROM source_ent_pro
)
SELECT * FROM treats