

{{
    config( materialized = 'incremental' )

}}



WITH source_int_estoque
    AS (
        SELECT
            *
        FROM {{ ref( 'int_estoque' ) }} sis
            {% if is_incremental() %}
                WHERE sis."CD_ESTOQUE" > (SELECT MAX("CD_ESTOQUE") FROM {{ this }}) -- Carregar apenas novos dados
            {% endif %}
),
mrt_estoque
    AS (
        SELECT DISTINCT
            *
        FROM source_int_estoque
)
SELECT * FROM mrt_estoque
