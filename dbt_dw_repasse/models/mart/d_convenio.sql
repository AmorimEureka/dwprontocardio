{{

    config( materialized = 'incremental',
            unique_key = 'cd_convenio',
            on_schema_change = 'sync_all_columns',
            tags = ['repasse']
    )
}}

WITH source_int_convenio
    AS (
        SELECT
            sis.cd_convenio,
            sis.nm_convenio
        FROM {{ ref( 'int_convenio' ) }} sis
        {% if is_incremental() %}
        WHERE sis.cd_convenio > ( SELECT MAX(cd_convenio) FROM {{ this }} )
        {% endif %}
),
treats
    AS (
        SELECT * FROM source_int_convenio
)
SELECT * FROM treats