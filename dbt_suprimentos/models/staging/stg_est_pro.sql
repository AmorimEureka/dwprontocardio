{{
    config( materialized = 'incremental',
            unique_key = '"CD_EST_PRO_KEY"' )
}}

WITH source_est_pro
    AS (
        SELECT
            NULLIF("CD_ESTOQUE", 'NaN') AS "CD_ESTOQUE"
            , NULLIF("CD_PRODUTO", 'NaN') AS "CD_PRODUTO"
            , NULLIF("CD_LOCALIZACAO", 'NaN') AS "CD_LOCALIZACAO"
            , "DS_LOCALIZACAO_PRATELEIRA"
            , "DT_ULTIMA_MOVIMENTACAO"
            , NULLIF("QT_ESTOQUE_ATUAL", 'NaN') AS "QT_ESTOQUE_ATUAL"
            , NULLIF("QT_ESTOQUE_MAXIMO", 'NaN') AS "QT_ESTOQUE_MAXIMO"
            , NULLIF("QT_ESTOQUE_MINIMO", 'NaN') AS "QT_ESTOQUE_MINIMO"
            , NULLIF("QT_ESTOQUE_VIRTUAL", 'NaN') AS "QT_ESTOQUE_VIRTUAL"
            , NULLIF("QT_PONTO_DE_PEDIDO", 'NaN') AS"QT_PONTO_DE_PEDIDO"
            , NULLIF("QT_CONSUMO_MES", 'NaN') AS "QT_CONSUMO_MES"
            , NULLIF("QT_SOLICITACAO_DE_COMPRA", 'NaN') AS "QT_SOLICITACAO_DE_COMPRA"
            , NULLIF("QT_ORDEM_DE_COMPRA", 'NaN') AS "QT_ORDEM_DE_COMPRA"
            , NULLIF("QT_ESTOQUE_DOADO", 'NaN') AS "QT_ESTOQUE_DOADO"
            , NULLIF("QT_ESTOQUE_RESERVADO", 'NaN') AS "QT_ESTOQUE_RESERVADO"
            , NULLIF("QT_CONSUMO_ATUAL", 'NaN') AS "QT_CONSUMO_ATUAL"
            , "TP_CLASSIFICACAO_ABC"
            , "DT_EXTRACAO"
        FROM {{ source('raw_mv' , 'est_pro') }}
),
treats_key
    AS (
        SELECT
            CONCAT(sis."CD_ESTOQUE", sis."CD_PRODUTO") AS "CD_EST_PRO_KEY"
            , sis."CD_ESTOQUE"
            , sis."CD_PRODUTO"
            , sis."CD_LOCALIZACAO"
            , sis."DS_LOCALIZACAO_PRATELEIRA"
            , sis."DT_ULTIMA_MOVIMENTACAO"
            , sis."QT_ESTOQUE_ATUAL"
            , sis."QT_ESTOQUE_MAXIMO"
            , sis."QT_ESTOQUE_MINIMO"
            , sis."QT_ESTOQUE_VIRTUAL"
            , sis."QT_PONTO_DE_PEDIDO"
            , sis."QT_CONSUMO_MES"
            , sis."QT_SOLICITACAO_DE_COMPRA"
            , sis."QT_ORDEM_DE_COMPRA"
            , sis."QT_ESTOQUE_DOADO"
            , sis."QT_ESTOQUE_RESERVADO"
            , sis."QT_CONSUMO_ATUAL"
            , sis."TP_CLASSIFICACAO_ABC"
            , sis."DT_EXTRACAO"
        FROM source_est_pro sis
        {% if is_incremental() %}
        WHERE CONCAT(sis."CD_ESTOQUE", sis."CD_PRODUTO")::BIGINT > ( SELECT MAX("CD_EST_PRO_KEY") FROM {{this}} )
        {% endif %}
),
treats
    AS(
        SELECT
            "CD_EST_PRO_KEY"::BIGINT
            , "CD_ESTOQUE"::BIGINT
            , "CD_PRODUTO"::BIGINT
            , "CD_LOCALIZACAO"::VARCHAR(20)
            , "DS_LOCALIZACAO_PRATELEIRA"::VARCHAR(30)
            , "DT_ULTIMA_MOVIMENTACAO"::TIMESTAMP
            , "QT_ESTOQUE_ATUAL"::NUMERIC(11,3)
            , "QT_ESTOQUE_MAXIMO"::NUMERIC(11,3)
            , "QT_ESTOQUE_MINIMO"::NUMERIC(11,3)
            , "QT_ESTOQUE_VIRTUAL"::NUMERIC(11,3)
            , "QT_PONTO_DE_PEDIDO"::NUMERIC(11,3)
            , "QT_CONSUMO_MES"::NUMERIC(11,3)
            , "QT_SOLICITACAO_DE_COMPRA"::NUMERIC(11,3)
            , "QT_ORDEM_DE_COMPRA"::NUMERIC(11,3)
            , "QT_ESTOQUE_DOADO"::NUMERIC(11,3)
            , "QT_ESTOQUE_RESERVADO"::NUMERIC(11,3)
            , "QT_CONSUMO_ATUAL"::NUMERIC(11,3)
            , "TP_CLASSIFICACAO_ABC"::VARCHAR(1)
            , "DT_EXTRACAO"::TIMESTAMP
        FROM treats_key
)
SELECT * FROM treats