{{
    config( materialized = 'incremental',
            unique_key = '"CD_PACIENTE"' )
}}

WITH source_paciente
    AS (
        SELECT
            NULLIF(sis."CD_PACIENTE", 'NaN') AS "CD_PACIENTE",
            sis."NM_PACIENTE",
            sis."DT_EXTRACAO"
        FROM {{ source('raw_mv', 'paciente')}} sis
        {% if is_incremental() %}
        WHERE sis."CD_PACIENTE"::BIGINT > ( SELECT MAX("CD_PACIENTE") FROM {{this}} )
        {% endif %}
),
treats
    AS (
        SELECT
            "CD_PACIENTE"::BIGINT AS "CD_PACIENTE",
            "NM_PACIENTE",
            "DT_EXTRACAO"::TIMESTAMP AS "DT_EXTRACAO"
        FROM source_paciente
)
SELECT * FROM treats