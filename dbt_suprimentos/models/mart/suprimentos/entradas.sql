

{{
    config( materialized = 'incremental' )

}}



WITH source_int_entradas
    AS (
        SELECT
            *
        FROM {{ ref( 'int_entradas' ) }} sis
            {% if is_incremental() %}
                WHERE sis."CD_ENT_PRO" > (SELECT MAX("CD_ENT_PRO") FROM {{ this }}) -- Carregar apenas novos dados
            {% endif %}
),
mrt_entradas
    AS (
        SELECT
            *
        FROM source_int_entradas
)
SELECT * FROM mrt_entradas
