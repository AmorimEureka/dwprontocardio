{{

    config( materialized = 'incremental',
            unique_key = 'cd_procedimento',
            on_schema_change = 'sync_all_columns',
            tags = ['repasse']
    )
}}

WITH source_int_procedimento
    AS (
        SELECT
            sis.cd_procedimento::VARCHAR(21) AS cd_procedimento,
            MAX(sis.ds_procedimento::VARCHAR(250)) AS ds_procedimento
        FROM {{ ref('int_procedimento') }} sis
        WHERE sis.cd_procedimento IS NOT NULL
        GROUP BY 1
),
source_incremental
    AS (
        SELECT
            sis.cd_procedimento,
            sis.ds_procedimento
        FROM source_int_procedimento sis
        {% if is_incremental() %}
        WHERE NOT EXISTS (
            SELECT 1
            FROM {{ this }} tgt
            WHERE tgt.cd_procedimento = sis.cd_procedimento
        )
        {% endif %}
),
treats
    AS (
        SELECT
            cd_procedimento,
            ds_procedimento
        FROM source_incremental
)
SELECT * FROM treats