{{
    config( materialized = 'incremental',
            unique_key = '"CD_ATENDIMENTO"' )
}}


WITH source_atendime
    AS (
        SELECT
            NULLIF(sis."CD_ATENDIMENTO", 'NaN') AS "CD_ATENDIMENTO",
            NULLIF(sis."CD_PACIENTE", 'NaN') AS "CD_PACIENTE",
            sis."DT_ATENDIMENTO",
            sis."DT_EXTRACAO"
        FROM {{ source('raw_mv', 'atendime')}} sis
        {% if is_incremental() %}
        WHERE sis."CD_ATENDIMENTO"::BIGINT > ( SELECT MAX("CD_ATENDIMENTO") FROM {{this}} )
        {% endif %}
),
treats
    AS (
        SELECT
            "CD_ATENDIMENTO"::BIGINT AS "CD_ATENDIMENTO",
            "CD_PACIENTE"::BIGINT AS "CD_PACIENTE",
            "DT_ATENDIMENTO"::TIMESTAMP AS "DT_ATENDIMENTO",
            "DT_EXTRACAO"::TIMESTAMP AS "DT_EXTRACAO"
        FROM source_atendime
)
SELECT * FROM treats