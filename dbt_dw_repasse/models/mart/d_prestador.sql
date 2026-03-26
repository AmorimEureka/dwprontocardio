{{

    config( materialized = 'incremental',
            unique_key = 'cd_prestador',
            on_schema_change = 'sync_all_columns',
            tags = ['repasse']
    )
}}

WITH source_int_prestador
    AS (
        SELECT
            sis.cd_prestador,
            sis.nm_prestador
        FROM {{ ref('int_prestador') }} sis
        {% if is_incremental() %}
        WHERE sis.cd_prestador > ( SELECT MAX(cd_prestador) FROM {{ this }} )
        {% endif %}
),
treats
    AS (
        SELECT * FROM source_int_prestador
)
SELECT * FROM treats