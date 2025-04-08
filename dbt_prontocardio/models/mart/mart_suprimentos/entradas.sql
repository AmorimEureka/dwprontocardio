

{{
    config( materialized = 'incremental',
            unique_key = '"CD_ITENT_PRO"'  )

}}



WITH source_int_entradas
    AS (
        SELECT
            *
        FROM {{ ref( 'int_suprimento_entradas' ) }} sis
        {% if is_incremental() %}
        WHERE sis."CD_ITENT_PRO" > (SELECT MAX("CD_ITENT_PRO") FROM {{ this }})
        {% endif %}
),
mrt_entradas
    AS (
        SELECT
            *
        FROM source_int_entradas
)
SELECT * FROM mrt_entradas
