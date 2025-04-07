{{
    config( materialized = 'incremental',
            unique_key = '"CD_REPASSE"' )
}}

WITH source_repasse
    AS (
        SELECT
            NULLIF(sis."CD_REPASSE", 'NaN') AS "CD_REPASSE",
            sis."CD_PRESTADOR_REPASSE",
            sis."DS_REPASSE",
            sis."DT_COMPETENCIA",
            sis."DT_REPASSE",
            sis."TP_REPASSE",
            sis."VL_REPASSE",
            sis."DT_EXTRACAO"
        FROM {{ source('raw_mv', 'repasse_prestador')}} sis
        {% if is_incremental() %}
        WHERE sis."CD_REPASSE"::BIGINT > ( SELECT MAX("CD_REPASSE") FROM {{this}} )
        {% endif %}
),
treats
    AS (
        SELECT
            "CD_REPASSE"::BIGINT,
            "CD_PRESTADOR_REPASSE"::BIGINT,
            "DS_REPASSE"::VARCHAR(250),
            "DT_COMPETENCIA"::TIMESTAMP,
            "DT_REPASSE"::TIMESTAMP,
            "TP_REPASSE"::VARCHAR(1),
            "VL_REPASSE"::NUMERIC(12, 2),
            "DT_EXTRACAO"::TIMESTAMP
        FROM  source_repasse
)
SELECT * FROM treats