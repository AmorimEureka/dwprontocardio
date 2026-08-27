{{
    config( materialized = 'incremental',
            unique_key = 'cd_repasse',
            on_schema_change = 'sync_all_columns',
            tags = ['repasse']
    )
}}

WITH source_repasse
    AS (
        SELECT
            sis.cd_repasse,
            sis.dt_competencia,
            sis.dt_repasse,
            sis.tp_repasse
        FROM {{ source('raw_repasse_mv', 'repasse')}} sis
),
treats
    AS (
        SELECT
            cd_repasse::BIGINT,
            dt_competencia::TIMESTAMP,
            dt_repasse::TIMESTAMP,
            tp_repasse::VARCHAR(1)
        FROM  source_repasse
)
SELECT * FROM treats
