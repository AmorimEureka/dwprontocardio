{{

    config( materialized = 'incremental',
            unique_key = 'cd_gru_fat',
            on_schema_change = 'sync_all_columns',
            tags = ['repasse']
    )
}}

WITH source_int_grupo_faturamento
    AS (
        SELECT
            sis.cd_gru_fat,
            sis.ds_gru_fat
        FROM {{ ref('int_grupo_faturamento') }} sis
        {% if is_incremental() %}
        WHERE sis.cd_gru_fat > ( SELECT MAX(cd_gru_fat) FROM {{ this }} )
        {% endif %}
),
treats
    AS (
        SELECT * FROM source_int_grupo_faturamento
)
SELECT * FROM treats