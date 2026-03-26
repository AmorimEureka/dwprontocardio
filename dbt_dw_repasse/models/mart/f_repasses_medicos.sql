{{

    config( materialized = 'incremental',
            unique_key = 'cd_itreg_key',
            merge_update_columns = ['cd_remessa', 'sn_fechada', 'dt_fechamento'],
            on_schema_change = 'sync_all_columns',
            tags = ['repasse']
    )
}}

WITH source_int_repasses_medicos
    AS (
        SELECT
            *
        FROM {{ ref('int_repasses_medicos') }} sis
),
source_incremental
    AS (
        SELECT
            sis.*
        FROM source_int_repasses_medicos sis
        {% if is_incremental() %}
        LEFT JOIN {{ this }} tgt
            ON tgt.cd_itreg_key = sis.cd_itreg_key
        WHERE tgt.cd_itreg_key IS NULL
            OR tgt.cd_repasse IS DISTINCT FROM sis.cd_repasse
            OR tgt.sn_fechada IS DISTINCT FROM sis.sn_fechada
            OR tgt.dt_fechamento IS DISTINCT FROM sis.dt_fechamento
        {% endif %}
),
mrt_repasses_medicos
    AS (
        SELECT
            *
        FROM source_incremental
)
SELECT * FROM mrt_repasses_medicos