{{
    config(
        tags = ['repasse']
    )
}}

WITH source_convenio
    AS (
        SELECT
            cd_convenio,
            nm_convenio
        FROM {{ ref('stg_convenio') }}
),
treats
    AS (
        SELECT
            cd_convenio,
            nm_convenio
        FROM source_convenio
)
SELECT * FROM treats