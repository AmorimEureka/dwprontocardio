{{

    config( materialized = 'incremental',
            unique_key = '"CD_GRU_PRO"' )

}}

WITH source_int_grupo_procedimento
    AS (
        SELECT
            sis."CD_GRU_PRO",
            sis."DS_GRU_PRO"
        FROM {{ ref('int_grupo_procedimento') }} sis
        {% if is_incremental() %}
        WHERE sis."CD_GRU_PRO" > ( SELECT MAX("CD_GRU_PRO") FROM {{ this }} )
        {% endif %}
),
treats
    AS (
        SELECT * FROM source_int_grupo_procedimento
)
SELECT * FROM treats