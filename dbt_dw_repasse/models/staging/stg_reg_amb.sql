{{
    config( materialized = 'incremental',
            unique_key = 'cd_reg_amb',
            merge_update_columns = ['cd_remessa', 'dt_remessa'],
        on_schema_change = 'sync_all_columns',
        tags = ['repasse']
    )
}}

WITH source_reg_amb
    AS (
        SELECT
            sis.cd_reg_amb,
            sis.cd_remessa,
            sis.dt_lancamento AS dt_reg_amb,
            sis.dt_remessa
        FROM {{ source('raw_repasse_mv', 'reg_amb')}} sis
        {% if is_incremental() %}
        WHERE sis.cd_reg_amb::BIGINT > ( SELECT MAX(cd_reg_amb) FROM {{ this }} )
        {% endif %}
),
treats
    AS (
        SELECT
            cd_reg_amb::BIGINT,
            cd_remessa::BIGINT,
            dt_reg_amb::TIMESTAMP,
            dt_remessa::TIMESTAMP
        FROM  source_reg_amb
)
SELECT * FROM treats