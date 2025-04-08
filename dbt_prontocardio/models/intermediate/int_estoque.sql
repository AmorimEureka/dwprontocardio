

WITH source_estoque
    AS (
        SELECT 
            "CD_ESTOQUE"
            , "CD_SETOR"
            , "DS_ESTOQUE"
            , "TP_ESTOQUE"
        FROM {{ ref( 'stg_estoque' ) }}
),
treats
    AS (
        SELECT 
            *
        FROM source_estoque

)
SELECT * FROM treats