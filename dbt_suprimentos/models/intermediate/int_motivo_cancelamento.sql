

WITH source_mot_cancel
    AS (
        SELECT 
            "CD_MOT_CANCEL"
            , "DS_MOT_CANCEL"
            , "TP_MOT_CANCEL"
        FROM {{ ref( 'stg_mot_cancel' ) }}
),
treats
    AS (
        SELECT 
            *
        FROM source_mot_cancel
)
SELECT * FROM treats