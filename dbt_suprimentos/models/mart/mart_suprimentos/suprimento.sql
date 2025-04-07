{{
    config( materialized = 'incremental',
            unique_key = '"CD_SUPRIMENTO_KEY"' )
}}

WITH source_int_suprimento
    AS (
        SELECT
            *
        FROM {{ ref( 'int_suprimento' ) }} sis
        {% if is_incremental() %}
        WHERE sis."CD_SUPRIMENTO_KEY" > ( SELECT MAX("CD_SUPRIMENTO_KEY") FROM {{this}} )
        {% endif %}
),
mrt_suprimento
    AS (
        SELECT
            *
        FROM source_int_suprimento
)
SELECT * FROM mrt_suprimento