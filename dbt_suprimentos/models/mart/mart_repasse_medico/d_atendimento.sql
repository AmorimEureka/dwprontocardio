{{

    config( materialized = 'incremental',
            unique_key = '"CD_ATENDIMENTO"' )

}}

WITH source_int_atendimento
    AS (
        SELECT
            sis."CD_ATENDIMENTO",
            sis."CD_PACIENTE",
            sis."DT_ATENDIMENTO"
        FROM {{ ref( 'int_atendimento' ) }} sis
        {% if is_incremental() %}
        WHERE sis."CD_ATENDIMENTO" > ( SELECT MAX("CD_ATENDIMENTO") FROM {{ this }} )
        {% endif %}
),
treats
    AS (
        SELECT * FROM source_int_atendimento
)
SELECT * FROM treats