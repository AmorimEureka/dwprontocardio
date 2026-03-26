{{
    config(
        tags = ['repasse']
    )
}}

WITH source_prestador
    AS (
        SELECT
            cd_prestador,
            nm_prestador
        FROM {{ ref('stg_prestador') }}
),
treats
    AS (
        SELECT
            cd_prestador,
            nm_prestador
        FROM source_prestador
)
SELECT * FROM treats