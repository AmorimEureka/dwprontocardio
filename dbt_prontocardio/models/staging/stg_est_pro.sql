{{
    config( materialized = 'incremental',
            incremental_strategy = 'merge',
            unique_key = '"CD_EST_PRO_KEY"',
            merge_update_columns = ['"QT_ESTOQUE_ATUAL"', '"QT_CONSUMO_ATUAL"', '"QT_MVTO"',
                                    '"QT_MENSAL"', '"QT_DIARIO"', '"QTD_DIAS_ESTOQUE"', '"DT_ULTIMA_MOVIMENTACAO"'] )
}}

WITH source_est_pro
    AS (
        SELECT
            CONCAT(sis."CD_ESTOQUE", sis."CD_PRODUTO") AS "CD_EST_PRO_KEY"
            , NULLIF(sis."CD_ESTOQUE", 'NaN') AS "CD_ESTOQUE"
            , NULLIF(sis."CD_PRODUTO", 'NaN') AS "CD_PRODUTO"
            , NULLIF(sis."CD_LOCALIZACAO", 'NaN') AS "CD_LOCALIZACAO"
            , NULLIF(sis."TP_CLASSIFICACAO_ABC", 'NaN') AS "TP_CLASSIFICACAO_ABC"
            , sis."DT_ULTIMA_MOVIMENTACAO"
            , NULLIF(sis."QT_ESTOQUE_ATUAL", 'NaN') AS "QT_ESTOQUE_ATUAL"
            , NULLIF(sis."QT_CONSUMO_ATUAL", 'NaN') AS "QT_CONSUMO_ATUAL"
            , NULLIF(sis."QT_MVTO", 'NaN') AS "QT_MVTO"
            , NULLIF(sis."QT_MENSAL", 'NaN') AS "QT_MENSAL"
            , NULLIF(sis."QT_DIARIO", 'NaN') AS "QT_DIARIO"
            , NULLIF(SPLIT_PART(sis."QTD_DIAS_ESTOQUE"::TEXT, '.', 1), 'NaN') AS "QTD_DIAS_ESTOQUE"
            , sis."DT_EXTRACAO"
        FROM {{ source('raw_mv' , 'est_pro') }} sis
        {% if is_incremental() %}
        WHERE (
                sis."DT_ULTIMA_MOVIMENTACAO" >= (current_date - interval '5 day')
                OR
                CONCAT(sis."CD_ESTOQUE", sis."CD_PRODUTO")::BIGINT > ( SELECT MAX("CD_EST_PRO_KEY") FROM {{this}} )
                )
        {% endif %}
),
treats
    AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY "CD_EST_PRO_KEY" ORDER BY "DT_ULTIMA_MOVIMENTACAO" DESC) as rn
        FROM (
            SELECT
                "CD_EST_PRO_KEY"::BIGINT
                , "CD_ESTOQUE"::BIGINT
                , "CD_PRODUTO"::BIGINT
                , "CD_LOCALIZACAO"::VARCHAR(20)
                , "TP_CLASSIFICACAO_ABC"::VARCHAR(1)
                , "DT_ULTIMA_MOVIMENTACAO"::TIMESTAMP
                , "QT_ESTOQUE_ATUAL"::NUMERIC(11,3)
                , "QT_CONSUMO_ATUAL"::NUMERIC(11,3)
                , "QT_MVTO"::NUMERIC(11,3)
                , "QT_MENSAL"::NUMERIC(11,3)
                , "QT_DIARIO"::NUMERIC(11,3)
                , "QTD_DIAS_ESTOQUE"::BIGINT
                , "DT_EXTRACAO"::TIMESTAMP
            FROM source_est_pro
        ) base
    )
SELECT * FROM treats WHERE rn = 1