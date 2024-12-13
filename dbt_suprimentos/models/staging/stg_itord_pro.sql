

WITH source_itord_pro 
    AS (
        SELECT
            NULLIF("CD_ORD_COM", 'NaN') AS "CD_ORD_COM"
            , NULLIF("CD_PRODUTO", 'NaN') AS "CD_PRODUTO"
            , NULLIF("CD_UNI_PRO", 'NaN') AS "CD_UNI_PRO"
            , NULLIF(SPLIT_PART("CD_MOT_CANCEL"::TEXT, '.', 1), 'NaN') AS "CD_MOT_CANCEL"
            , "DT_CANCEL"
            , NULLIF("QT_COMPRADA", 'NaN') AS "QT_COMPRADA"
            , NULLIF("QT_ATENDIDA", 'NaN') AS "QT_ATENDIDA"
            , NULLIF("QT_RECEBIDA", 'NaN') AS "QT_RECEBIDA"
            , NULLIF("QT_CANCELADA", 'NaN') AS "QT_CANCELADA"
            , NULLIF("VL_UNITARIO", 'NaN') AS "VL_UNITARIO"
            , NULLIF("VL_TOTAL", 'NaN') AS "VL_TOTAL"
            , NULLIF("VL_CUSTO_REAL", 'NaN') AS "VL_CUSTO_REAL"
            , NULLIF("VL_TOTAL_CUSTO_REAL", 'NaN') AS "VL_TOTAL_CUSTO_REAL"
            , "DT_EXTRACAO"
        FROM {{ source('raw_mv' , 'itord_pro') }}
),
treats
    AS (
        SELECT
            "CD_ORD_COM"::BIGINT
            , "CD_PRODUTO"::BIGINT
            , "CD_UNI_PRO"::BIGINT
            , "CD_MOT_CANCEL"::BIGINT
            , "DT_CANCEL"::TIMESTAMP
            , "QT_COMPRADA"::NUMERIC(11,3)
            , "QT_ATENDIDA"::NUMERIC(11,3)
            , "QT_RECEBIDA"::NUMERIC(11,3)
            , "QT_CANCELADA"::NUMERIC(11,3)
            , "VL_UNITARIO"::NUMERIC(12,2)
            , "VL_TOTAL"::NUMERIC(12,2)
            , "VL_CUSTO_REAL"::NUMERIC(12,2)
            , "VL_TOTAL_CUSTO_REAL"::NUMERIC(12,2)
            , "DT_EXTRACAO"::TIMESTAMP
        FROM source_itord_pro
    )
SELECT * FROM treats