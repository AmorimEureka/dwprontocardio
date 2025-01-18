{{
    config( materialized = 'table' )

}}

WITH source_ent_pro 
    AS (
        SELECT
            NULLIF("CD_ENT_PRO", 'NaN') AS "CD_ENT_PRO"  
            , NULLIF("CD_TIP_ENT", 'NaN') AS "CD_TIP_ENT"  
            , NULLIF("CD_ESTOQUE", 'NaN') AS "CD_ESTOQUE"  
            , NULLIF("CD_FORNECEDOR", 'NaN') AS "CD_FORNECEDOR"  
            , NULLIF(SPLIT_PART("CD_ORD_COM"::TEXT,'.', 1), 'NaN') AS "CD_ORD_COM"  
            , NULLIF("CD_USUARIO_RECEBIMENTO", 'NaN') AS "CD_USUARIO_RECEBIMENTO"  
            , NULLIF("CD_ATENDIMENTO", 'NaN') AS "CD_ATENDIMENTO"  
            , "DT_EMISSAO"  
            , "DT_ENTRADA"  
            , "DT_RECEBIMENTO"  
            , "HR_ENTRADA"  
            , NULLIF("VL_TOTAL", 'NaN') AS "VL_TOTAL"  
            , NULLIF("NR_DOCUMENTO", 'NaN') AS "NR_DOCUMENTO"  
            , NULLIF("NR_CHAVE_ACESSO", 'NaN') AS "NR_CHAVE_ACESSO"  
            , NULLIF("SN_AUTORIZADO", 'NaN') AS "SN_AUTORIZADO"  
            , "DT_EXTRACAO"  
        FROM {{ source('raw_mv' , 'ent_pro') }}
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