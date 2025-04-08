{{

    config( materialized = 'incremental',
            unique_key = '"CD_PACIENTE"' )

}}

WITH source_int_paciente
    AS (
        SELECT
            sis."CD_PACIENTE",
            sis."NM_PACIENTE"
        FROM {{ ref( 'int_paciente' ) }} sis
        {% if is_incremental() %}
        WHERE sis."CD_PACIENTE" > ( SELECT MAX("CD_PACIENTE") FROM {{ this }} )
        {% endif %}
),
treats
    AS (
        SELECT * FROM source_int_paciente
)
SELECT * FROM treats