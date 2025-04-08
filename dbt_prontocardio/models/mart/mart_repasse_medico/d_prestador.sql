{{

    config( materialized = 'incremental',
            unique_key = '"CD_PRESTADOR"' )

}}

WITH source_int_prestador
    AS (
        SELECT
            sis."CD_PRESTADOR",
            sis."NM_PRESTADOR"
        FROM {{ ref('int_prestador') }} sis
        {% if is_incremental() %}
        WHERE sis."CD_PRESTADOR" > ( SELECT MAX("CD_PRESTADOR") FROM {{ this }} )
        {% endif %}
),
treats
    AS (
        SELECT * FROM source_int_prestador
)
SELECT * FROM treats