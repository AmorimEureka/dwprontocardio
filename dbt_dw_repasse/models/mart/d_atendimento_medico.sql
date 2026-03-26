{{

    config( materialized = 'incremental',
            unique_key = 'cd_ati_med',
            on_schema_change = 'sync_all_columns',
            tags = ['repasse']
    )
}}

WITH source_int_atendimento_medico
    AS (
        SELECT
            sis.cd_ati_med,
            sis.ds_ati_med
        FROM {{ ref('int_atendimento_medico') }} sis
        {% if is_incremental() %}
        WHERE sis.cd_ati_med > ( SELECT MAX(cd_ati_med) FROM {{ this }} )
        {% endif %}
),
treats
    AS (
        SELECT * FROM source_int_atendimento_medico
)
SELECT * FROM treats