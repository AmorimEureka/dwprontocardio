

{{

    config( materialized = 'incremental',
            unique_key = '"CD_PRESTADOR"'
            )

}}

WITH source_prestador
    AS (
        SELECT
            sis."CD_PRESTADOR",
            sis."NM_PRESTADOR",
            sis."DT_EXTRACAO"
        FROM {{ source('raw_mv', 'prestador') }} sis
        {% if is_incremental() %}
        WHERE sis."CD_PRESTADOR"::BIGINT > ( SELECT MAX("CD_PRESTADOR") FROM {{ this }} )
        {% endif %}
),
treats
    AS (
        SELECT
            "CD_PRESTADOR"::BIGINT,
            "NM_PRESTADOR"::VARCHAR(40),
            "DT_EXTRACAO"::TIMESTAMP
        FROM source_prestador
)
SELECT * FROM treats