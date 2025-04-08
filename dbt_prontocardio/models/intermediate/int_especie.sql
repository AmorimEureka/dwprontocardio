




WITH source_especie 
    AS (
        SELECT 
            "CD_ESPECIE"
            , "CD_ITEM_RES"
            , "DS_ESPECIE"
        FROM {{ ref( 'stg_especie' ) }}
),
treats
    AS (
        SELECT
            *
        FROM source_especie

)
SELECT * FROM treats