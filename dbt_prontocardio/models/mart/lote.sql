

{{
    config( materialized = 'incremental' )

}}



WITH source_int_lote
    AS (
        SELECT
            *
        FROM {{ ref( 'int_lot_pro' ) }} sis
            {% if is_incremental() %}
                WHERE sis."CD_LOT_PRO" > (SELECT MAX("CD_LOT_PRO") FROM {{ this }}) -- Carregar apenas novos dados
            {% endif %}
),
mrt_lote
    AS (
        SELECT
            *
        FROM source_int_lote
)
SELECT * FROM mrt_lote
