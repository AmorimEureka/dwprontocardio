{{

    config( materialized = 'incremental',
            unique_key = '"CD_GRU_FAT"' )

}}

WITH source_int_grupo_faturamento
    AS (
        SELECT
            sis."CD_GRU_FAT",
            sis."DS_GRU_FAT"
        FROM {{ ref('int_grupo_faturamento') }} sis
        {% if is_incremental() %}
        WHERE sis."CD_GRU_FAT" > ( SELECT MAX("CD_GRU_FAT") FROM {{ this }} )
        {% endif %}
),
treats
    AS (
        SELECT * FROM source_int_grupo_faturamento
)
SELECT * FROM treats