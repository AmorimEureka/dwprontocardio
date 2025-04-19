{{

    config( materialized = 'incremental',
            unique_key = '"CD_PROCEDIMENTO"' )

}}

WITH source_int_procedimento
    AS (
        SELECT
            sis."CD_PROCEDIMENTO",
            sis."DS_PROCEDIMENTO"
        FROM {{ ref('int_procedimento') }} sis
        {% if is_incremental() %}
        WHERE sis."CD_PROCEDIMENTO" > ( SELECT MAX("CD_PROCEDIMENTO") FROM {{ this }} )
        {% endif %}
),
treats
    AS (
        SELECT * FROM source_int_procedimento
)
SELECT * FROM treats