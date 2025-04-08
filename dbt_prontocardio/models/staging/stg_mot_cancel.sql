{{
    config( materialized = 'incremental',
            unique_key = '"CD_MOT_CANCEL"' )
}}

WITH source_mot_cancel
    AS (
        SELECT
            NULLIF(sis."CD_MOT_CANCEL", 'NaN') AS "CD_MOT_CANCEL"
            , NULLIF(sis."DS_MOT_CANCEL", 'NaN') AS "DS_MOT_CANCEL"
            , NULLIF(sis."TP_MOT_CANCEL", 'NaN') AS "TP_MOT_CANCEL"
            , sis."DT_EXTRACAO"
        FROM {{ source('raw_mv' , 'mot_cancel') }} sis
        {% if is_incremental() %}
        WHERE sis."CD_MOT_CANCEL"::BIGINT > ( SELECT MAX("CD_MOT_CANCEL") FROM {{this}} )
        {% endif %}
),
treats
    AS (
        SELECT
            "CD_MOT_CANCEL"::BIGINT
            , "DS_MOT_CANCEL"::VARCHAR(60)
            , "TP_MOT_CANCEL"::VARCHAR(1)
            , "DT_EXTRACAO"::TIMESTAMP
        FROM source_mot_cancel
    )
SELECT * FROM treats