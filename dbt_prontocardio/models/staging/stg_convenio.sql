{{
    config( materialized = 'incremental',
            unique_key = '"CD_CONVENIO"' )
}}

WITH source_convenio
    AS (
        SELECT
            NULLIF(sis."CD_CONVENIO", 'NaN') AS "CD_CONVENIO",
            sis."NM_CONVENIO",
            sis."DT_EXTRACAO"
        FROM {{ source('raw_mv', 'convenio') }} sis
        {% if is_incremental() %}
        WHERE sis."CD_CONVENIO"::BIGINT > ( SELECT MAX("CD_CONVENIO") FROM {{this}} )
        {% endif %}
),
treats
    AS (
        SELECT
            "CD_CONVENIO"::BIGINT AS "CD_CONVENIO",
            "NM_CONVENIO"::VARCHAR(60) AS "NM_CONVENIO",
            "DT_EXTRACAO"::TIMESTAMP AS "DT_EXTRACAO"
        FROM source_convenio
)
SELECT * FROM treats