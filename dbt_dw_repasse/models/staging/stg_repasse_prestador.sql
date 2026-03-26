{{
    config( materialized = 'incremental',
            unique_key = 'cd_repasse',
            on_schema_change = 'sync_all_columns',
            tags = ['repasse']
    )
}}

WITH source_repasse_prestador
    AS (
        SELECT
            sis.cd_repasse,
            sis.cd_prestador_repasse,
            r.ds_repasse,
            r.dt_competencia,
            r.dt_repasse,
            r.tp_repasse,
            sis.vl_repasse
        FROM {{ source('raw_repasse_mv', 'repasse_prestador')}} sis
        INNER JOIN {{ source('raw_repasse_mv', 'repasse')}} r ON r.cd_repasse = sis.cd_repasse
        {% if is_incremental() %}
        WHERE sis.cd_repasse::BIGINT > ( SELECT MAX(cd_repasse) FROM {{this}} )
        {% endif %}
),
treats
    AS (
        SELECT
            cd_repasse::BIGINT,
            cd_prestador_repasse::BIGINT,
            ds_repasse::VARCHAR(250),
            dt_competencia::TIMESTAMP,
            dt_repasse::TIMESTAMP,
            tp_repasse::VARCHAR(1),
            vl_repasse::NUMERIC(12, 2)
        FROM  source_repasse_prestador
)
SELECT * FROM treats