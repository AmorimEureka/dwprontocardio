{{
    config( materialized = 'incremental',
            unique_key = 'cd_procedimento',
            on_schema_change = 'sync_all_columns',
            tags = ['repasse']
    )
}}

WITH source_procedimento_sus
    AS (
        SELECT
            sis.cd_procedimento,
            sis.ds_procedimento
        FROM {{ source('raw_repasse_mv', 'procedimento_sus')}} sis
        {% if is_incremental() %}
        WHERE (
                TRIM(sis.cd_procedimento::TEXT) ~ '^[0-9]+$'
                AND TRIM(sis.cd_procedimento::TEXT)::NUMERIC(20,0) > (
                    SELECT COALESCE(MAX(CASE
                        WHEN TRIM(cd_procedimento::TEXT) ~ '^[0-9]+$' THEN TRIM(cd_procedimento::TEXT)::NUMERIC(20,0)
                        ELSE NULL
                    END), 0)
                    FROM {{ this }}
                )
            )
            OR TRIM(sis.cd_procedimento::TEXT) !~ '^[0-9]+$'
        {% endif %}
),
treats
    AS (
        SELECT
            cd_procedimento::BIGINT AS cd_procedimento,
            ds_procedimento::VARCHAR(250)
        FROM source_procedimento_sus
)
SELECT * FROM treats
