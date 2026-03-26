{{

    config( materialized = 'incremental',
            unique_key = 'cd_gru_pro',
            on_schema_change = 'sync_all_columns',
            tags = ['repasse']
    )
}}

WITH source_int_grupo_procedimento
    AS (
        SELECT
            sis.cd_gru_pro,
            sis.ds_gru_pro
        FROM {{ ref('int_grupo_procedimento') }} sis
        {% if is_incremental() %}
        WHERE sis.cd_gru_pro > ( SELECT MAX(cd_gru_pro) FROM {{ this }} )
        {% endif %}
),
treats
    AS (
        SELECT * FROM source_int_grupo_procedimento
)
SELECT * FROM treats