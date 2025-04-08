

{{
    config( materialized = 'incremental' )

}}



WITH source_int_produto
    AS (
        SELECT
            *
        FROM {{ ref( 'int_produto' ) }} sis
            {% if is_incremental() %}
                WHERE sis."CD_PRODUTO" > (SELECT MAX("CD_PRODUTO") FROM {{ this }}) -- Carregar apenas novos dados
            {% endif %}
),
mrt_produto
    AS (
        SELECT
            *
        FROM source_int_produto
)
SELECT * FROM mrt_produto
