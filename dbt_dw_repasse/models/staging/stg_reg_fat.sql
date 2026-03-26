{{
    config( materialized = 'incremental',
            unique_key = 'cd_reg_fat',
            merge_update_columns = ['cd_remessa', 'dt_remessa', 'sn_fechada', 'dt_fechamento'],
            on_schema_change = 'sync_all_columns',
            tags = ['repasse']
    )
}}

WITH source_reg_fat
    AS (
        SELECT
            sis.cd_reg_fat,
            sis.cd_convenio,
            sis.cd_atendimento,
            sis.cd_remessa,
            sis.dt_inicio AS dt_reg_fat,
            sis.dt_fechamento,
            sis.dt_remessa,
            sis.sn_fechada
        FROM {{ source('raw_repasse_mv', 'reg_fat')}} sis
        {% if is_incremental() %}
        WHERE sis.cd_reg_fat::BIGINT > ( SELECT MAX(cd_reg_fat) FROM {{ this }} )
        {% endif %}
),
treats
    AS (
        SELECT
            cd_reg_fat::BIGINT,
            cd_convenio::BIGINT,
            cd_atendimento::BIGINT,
            cd_remessa::BIGINT,
            dt_reg_fat::TIMESTAMP,
            dt_fechamento::TIMESTAMP,
            dt_remessa::TIMESTAMP,
            sn_fechada::VARCHAR(1)
        FROM source_reg_fat
)
SELECT * FROM treats
