{{

    config( materialized = 'incremental',
            unique_key = '"CD_ATI_MED"' )

}}

WITH source_int_atendimento_medico
    AS (
        SELECT
            sis."CD_ATI_MED",
            sis."DS_ATI_MED"
        FROM {{ ref('int_atendimento_medico') }} sis
        {% if is_incremental() %}
        WHERE sis."CD_ATI_MED" > ( SELECT MAX("CD_ATI_MED") FROM {{ this }} )
        {% endif %}
),
treats
    AS (
        SELECT * FROM source_int_atendimento_medico
)
SELECT * FROM treats