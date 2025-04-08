


WITH source_produto 
    AS (
        SELECT 
            "CD_PRODUTO"
            , "CD_ESPECIE"
            , "DS_PRODUTO"
            , "DS_PRODUTO_RESUMIDO"
            , "DT_CADASTRO"
        FROM {{ ref( 'stg_produto' ) }}        
),
treats
    AS (
        SELECT 
            "CD_PRODUTO"
            , "CD_ESPECIE"
            , "DS_PRODUTO"
            , "DS_PRODUTO_RESUMIDO"
            , "DT_CADASTRO"
        FROM source_produto p
)
SELECT * FROM treats