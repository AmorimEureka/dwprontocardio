{{
    config( materialized = 'incremental',
            unique_key = '"CD_ITORD_PRO_KEY"' )

}}



WITH source_int_pedidos
    AS (
        SELECT
            *
        FROM {{ ref( 'int_suprimento_pedidos' ) }} sis
        {% if is_incremental() %}
        WHERE sis."CD_ITORD_PRO_KEY" > (SELECT MAX("CD_ITORD_PRO_KEY") FROM {{ this }})
        {% endif %}
),
mrt_pedidos
    AS (
        SELECT
            *
        FROM source_int_pedidos
)
SELECT * FROM mrt_pedidos
