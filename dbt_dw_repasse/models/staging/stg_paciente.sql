{{
    config( materialized = 'incremental',
            unique_key = 'cd_paciente',
        on_schema_change = 'sync_all_columns',
        tags = ['repasse']
    )
}}

WITH source_paciente
    AS (
        SELECT
            sis.cd_paciente,
            sis.nm_paciente
        FROM {{ source('raw_repasse_mv', 'paciente')}} sis
        {% if is_incremental() %}
        WHERE sis.cd_paciente::BIGINT > ( SELECT MAX(cd_paciente) FROM {{this}} )
        {% endif %}
),
treats
    AS (
        SELECT
            cd_paciente::BIGINT AS cd_paciente,
            nm_paciente
        FROM source_paciente
)
SELECT * FROM treats