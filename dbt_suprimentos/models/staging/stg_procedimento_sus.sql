{{
    config( materialized = 'incremental',
            unique_key = '"CD_PROCEDIMENTO"' )
}}

WITH source_procedimento_sus
    AS (
        SELECT
            NULLIF(sis."CD_PROCEDIMENTO", 'NaN') AS "CD_PROCEDIMENTO",
            sis."DS_PROCEDIMENTO",
            sis."DT_EXTRACAO"
        FROM {{ source('raw_mv', 'procedimento_sus')}} sis
        {% if is_incremental() %}
        WHERE sis."DT_EXTRACAO"::TIMESTAMP > ( SELECT MAX("DT_EXTRACAO") FROM {{this}} )
        {% endif %}
),
treats
    AS (
        SELECT
            "CD_PROCEDIMENTO"::VARCHAR(21) AS "CD_PROCEDIMENTO",
            "DS_PROCEDIMENTO"::VARCHAR(250),
            "DT_EXTRACAO"::TIMESTAMP AS "DT_EXTRACAO"
        FROM source_procedimento_sus
)
SELECT * FROM treats
