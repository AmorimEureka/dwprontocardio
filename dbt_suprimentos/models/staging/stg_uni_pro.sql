

WITH source_uni_pro 
    AS (
        SELECT
            NULLIF("CD_UNI_PRO", 'NaN') AS "CD_UNI_PRO"
            , NULLIF("CD_UNIDADE", 'NaN') AS "CD_UNIDADE"
            , NULLIF("CD_PRODUTO", 'NaN') AS "CD_PRODUTO"
            , NULLIF("VL_FATOR", 'NaN') AS "VL_FATOR"
            , NULLIF("DS_UNIDADE", 'NaN') AS "DS_UNIDADE"
            , NULLIF("TP_RELATORIOS", 'NaN') AS "TP_RELATORIOS"
            , NULLIF("SN_ATIVO", 'NaN') AS "SN_ATIVO"
            , "DT_EXTRACAO"
        FROM {{ source('raw_mv' , 'uni_pro') }}
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