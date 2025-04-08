{{
    config( materialized = 'incremental',
            unique_key = '"CD_LOT_PRO"' )
}}

WITH source_lot_pro
    AS (
        SELECT
            NULLIF(sis."CD_LOT_PRO", 'NaN') AS "CD_LOT_PRO"
            , NULLIF(sis."CD_ESTOQUE", 'NaN') AS "CD_ESTOQUE"
            , NULLIF(sis."CD_PRODUTO", 'NaN') AS "CD_PRODUTO"
            , NULLIF(sis."CD_LOTE", 'NaN') AS "CD_LOTE"
            , sis."DT_VALIDADE"
            , NULLIF(sis."QT_ESTOQUE_ATUAL", 'NaN') AS "QT_ESTOQUE_ATUAL"
            , sis."DT_EXTRACAO"
        FROM {{ source('raw_mv' , 'lot_pro') }} sis
        {% if is_incremental() %}
        WHERE sis."CD_LOT_PRO"::BIGINT > ( SELECT MAX("CD_LOT_PRO") FROM {{this}} )
        {% endif %}
),
treats
    AS (
        SELECT
            "CD_LOT_PRO"::BIGINT
            , "CD_ESTOQUE"::BIGINT
            , "CD_PRODUTO"::BIGINT
            , "CD_LOTE"::VARCHAR(555)
            , "DT_VALIDADE"::DATE
            , "QT_ESTOQUE_ATUAL"::NUMERIC(11,3)
            , "DT_EXTRACAO"::TIMESTAMP
        FROM source_lot_pro
    )
SELECT * FROM treats