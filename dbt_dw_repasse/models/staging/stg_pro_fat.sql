{{
    config( materialized = 'incremental',
            unique_key = 'cd_pro_fat',
        on_schema_change = 'sync_all_columns',
        tags = ['repasse']
    )
}}

WITH source_pro_fat
    AS (
        SELECT
            sis.cd_pro_fat,
            sis.cd_gru_pro,
            sis.ds_pro_fat
        FROM {{ source('raw_repasse_mv', 'pro_fat')}} sis
        {% if is_incremental() %}
        WHERE (
                TRIM(sis.cd_pro_fat) ~ '^[0-9]+$'
                AND TRIM(sis.cd_pro_fat)::NUMERIC(20,0) > (
                    SELECT COALESCE(MAX(CASE
                        WHEN TRIM(cd_pro_fat) ~ '^[0-9]+$' THEN TRIM(cd_pro_fat)::NUMERIC(20,0)
                        ELSE NULL
                    END), 0)
                    FROM {{ this }}
                )
            )
            OR TRIM(sis.cd_pro_fat) !~ '^[0-9]+$'
        {% endif %}
),
treats
    AS (
        SELECT
            cd_pro_fat::VARCHAR(21),
            cd_gru_pro::BIGINT,
            ds_pro_fat::VARCHAR(250)
        FROM source_pro_fat
)
SELECT * FROM treats