

{{
    config( materialized = 'incremental' )

}}



WITH source_int_especie
    AS (
        SELECT
            *
        FROM {{ ref( 'int_especie' ) }} sis
            {% if is_incremental() %}
                WHERE sis."CD_ESPECIE" > (SELECT MAX("CD_ESPECIE") FROM {{ this }}) -- Carregar apenas novos dados
            {% endif %}
),
mrt_especie
    AS (
        SELECT
            *
        FROM source_int_especie
)
SELECT * FROM mrt_especie
