WITH source_int_suprimento
    AS (
        SELECT
            *
        FROM {{ ref( 'int_suprimento' ) }}
),
mrt_suprimento
    AS (
        SELECT DISTINCT
            *
        FROM source_int_suprimento
)
SELECT * FROM mrt_suprimento
