{{

    config( materialized = 'incremental',
            unique_key = 'cd_paciente',
            on_schema_change = 'sync_all_columns',
            tags = ['repasse']
    )
}}

WITH source_int_paciente
    AS (
        SELECT
            sis.cd_paciente,
            sis.nm_paciente
        FROM {{ ref( 'int_paciente' ) }} sis
        {% if is_incremental() %}
        WHERE sis.cd_paciente > ( SELECT MAX(cd_paciente) FROM {{ this }} )
        {% endif %}
),
treats
    AS (
        SELECT * FROM source_int_paciente
)
SELECT * FROM treats