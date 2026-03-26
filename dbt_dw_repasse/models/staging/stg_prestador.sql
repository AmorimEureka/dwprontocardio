

{{

    config( materialized = 'incremental',
            unique_key = 'cd_prestador',
        on_schema_change = 'sync_all_columns',
        tags = ['repasse']
    )
}}

WITH source_prestador
    AS (
        SELECT
            sis.cd_prestador,
            sis.nm_prestador
        FROM {{ source('raw_repasse_mv', 'prestador') }} sis
        {% if is_incremental() %}
        WHERE sis.cd_prestador::BIGINT > ( SELECT MAX(cd_prestador) FROM {{ this }} )
        {% endif %}
),
treats
    AS (
        SELECT
            cd_prestador::BIGINT,
            nm_prestador::VARCHAR(40)
        FROM source_prestador
)
SELECT * FROM treats