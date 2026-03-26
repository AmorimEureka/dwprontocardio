{{
    config( materialized = 'incremental',
            unique_key = 'cd_convenio',
        on_schema_change = 'sync_all_columns',
        tags = ['repasse']
    )
}}

WITH source_convenio
    AS (
        SELECT
            sis.cd_convenio,
            sis.nm_convenio
        FROM {{ source('raw_repasse_mv', 'convenio') }} sis
        {% if is_incremental() %}
        WHERE sis.cd_convenio::BIGINT > ( SELECT MAX(cd_convenio) FROM {{this}} )
        {% endif %}
),
treats
    AS (
        SELECT
            cd_convenio::BIGINT AS cd_convenio,
            nm_convenio::VARCHAR(60) AS nm_convenio
        FROM source_convenio
)
SELECT * FROM treats