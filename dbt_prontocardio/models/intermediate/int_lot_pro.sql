
WITH source_lot_pro
    AS (
        SELECT 
            "CD_LOT_PRO"
            , "CD_ESTOQUE"
            , "CD_PRODUTO"
            , "CD_LOTE"
            , "DT_VALIDADE"
        FROM {{ ref( 'stg_lot_pro' ) }}       
)
SELECT * FROM source_lot_pro