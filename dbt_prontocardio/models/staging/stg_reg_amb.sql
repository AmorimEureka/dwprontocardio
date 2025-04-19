{{
    config( materialized = 'incremental',
            unique_key = '"CD_REG_AMB"',
            merge_update_columns = ['"CD_REMESSA", "DT_REMESSA"'] )
}}

WITH source_reg_amb
    AS (
        SELECT
            sis."CD_REG_AMB",
            NULLIF(SPLIT_PART(sis."CD_REMESSA", '.', 1), 'NaN') AS "CD_REMESSA",
            sis."DT_REG_AMB",
            sis."DT_REMESSA",
            sis."DT_EXTRACAO"
        FROM {{ source('raw_mv', 'reg_amb')}} sis
        {% if is_incremental() %}
        WHERE sis."CD_REG_AMB"::BIGINT > ( SELECT MAX("CD_REG_AMB") FROM {{ this }} )
        {% endif %}
),
treats
    AS (
        SELECT
            "CD_REG_AMB"::BIGINT,
            "CD_REMESSA"::BIGINT,
            "DT_REG_AMB"::TIMESTAMP,
            "DT_REMESSA"::TIMESTAMP,
            "DT_EXTRACAO"::TIMESTAMP
        FROM  source_reg_amb
)
SELECT * FROM treats