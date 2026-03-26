{{

    config( materialized = 'incremental',
            unique_key = 'cd_atendimento',
            on_schema_change = 'sync_all_columns',
            tags = ['repasse']
    )
}}

WITH source_int_atendimento
    AS (
        SELECT
            sis.cd_atendimento,
            sis.cd_paciente,
            sis.dt_atendimento
        FROM {{ ref( 'int_atendimento' ) }} sis
        {% if is_incremental() %}
        WHERE sis.cd_atendimento > ( SELECT MAX(cd_atendimento) FROM {{ this }} )
        {% endif %}
),
treats
    AS (
        SELECT * FROM source_int_atendimento
)
SELECT * FROM treats