{{

    config( materialized = 'incremental',
            unique_key = '"CD_CONVENIO"' )

}}

WITH source_int_convenio
    AS (
        SELECT
            sis."CD_CONVENIO",
            sis."NM_CONVENIO"
        FROM {{ ref( 'int_convenio' ) }} sis
        {% if is_incremental() %}
        WHERE sis."CD_CONVENIO" > ( SELECT MAX("CD_CONVENIO") FROM {{ this }} )
        {% endif %}
),
treats
    AS (
        SELECT * FROM source_int_convenio
)
SELECT * FROM treats