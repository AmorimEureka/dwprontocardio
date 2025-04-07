{{
    config( materialized = 'incremental',
            unique_key = '"CD_PRODUTO"',
            merge_update_columns = ['"DT_ULTIMA_ENTRADA"', '"HR_ULTIMA_ENTRADA"', '"QT_ESTOQUE_ATUAL"', '"QT_ULTIMA_ENTRADA"',
                                    '"VL_ULTIMA_ENTRADA"', '"VL_CUSTO_MEDIO"', '"VL_ULTIMA_CUSTO_REAL"', '"DT_EXTRACAO"' ] )
}}

WITH source_produto
    AS (
        SELECT
            NULLIF(sis."CD_PRODUTO", 'NaN') AS "CD_PRODUTO"
            , NULLIF(sis."CD_ESPECIE", 'NaN') AS "CD_ESPECIE"
            , NULLIF(sis."DS_PRODUTO", 'NaN') AS "DS_PRODUTO"
            , NULLIF(sis."DS_PRODUTO_RESUMIDO", 'NaN') AS "DS_PRODUTO_RESUMIDO"
            , sis."DT_CADASTRO"
            , sis."DT_ULTIMA_ENTRADA"
            , sis."HR_ULTIMA_ENTRADA"
            , NULLIF(sis."QT_ESTOQUE_ATUAL", 'NaN') AS "QT_ESTOQUE_ATUAL"
            , NULLIF(sis."QT_ULTIMA_ENTRADA", 'NaN') AS "QT_ULTIMA_ENTRADA"
            , NULLIF(sis."VL_ULTIMA_ENTRADA", 'NaN') AS "VL_ULTIMA_ENTRADA"
            , NULLIF(sis."VL_CUSTO_MEDIO", 'NaN') AS "VL_CUSTO_MEDIO"
            , NULLIF(sis."VL_ULTIMA_CUSTO_REAL", 'NaN') AS "VL_ULTIMA_CUSTO_REAL"
            , sis."DT_EXTRACAO"
        FROM {{ source('raw_mv' , 'produto') }} sis
        {% if is_incremental() %}
        WHERE sis."CD_PRODUTO"::BIGINT > ( SELECT MAX("CD_PRODUTO") FROM {{ this }} )
        {% endif %}
),
treats
    AS (
        SELECT
            "CD_PRODUTO"::BIGINT
            , "CD_ESPECIE"::BIGINT
            , "DS_PRODUTO"::VARCHAR(60)
            , "DS_PRODUTO_RESUMIDO"::VARCHAR(40)
            , "DT_CADASTRO"::TIMESTAMP
            , "DT_ULTIMA_ENTRADA"::DATE
            , "HR_ULTIMA_ENTRADA"::TIMESTAMP
            , "QT_ESTOQUE_ATUAL"::NUMERIC(11,3)
            , "QT_ULTIMA_ENTRADA"::NUMERIC(11,3)
            , "VL_ULTIMA_ENTRADA"::NUMERIC(12,2)
            , "VL_CUSTO_MEDIO"::NUMERIC(12,2)
            , "VL_ULTIMA_CUSTO_REAL"::NUMERIC(12,2)
            , "DT_EXTRACAO"::TIMESTAMP
        FROM source_produto
    )
SELECT * FROM treats