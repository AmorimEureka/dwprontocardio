

{{
    config( materialized = 'view' )

}}



WITH source_int_mov_estoque
    AS (
        SELECT
            *
        FROM {{ ref( 'int_mov_estoque' ) }}
),
mrt_mov_estoque
    AS (
        SELECT DISTINCT
            *
        FROM source_int_mov_estoque
)
SELECT * FROM mrt_mov_estoque
