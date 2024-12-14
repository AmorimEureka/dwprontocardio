{{
    config( materialized = 'incremental' )

}}



WITH source_int_pedidos
    AS (
        SELECT
            *
        FROM {{ ref( 'int_pedidos' ) }} sis
            {% if is_incremental() %}
                WHERE sis."CD_ORD_COM" > (SELECT MAX("CD_ORD_COM") FROM {{ this }}) -- Carregar apenas novos dados
            {% endif %}
),
mrt_pedidos
    AS (
        SELECT
            *
        FROM source_int_pedidos
)
SELECT * FROM mrt_pedidos
