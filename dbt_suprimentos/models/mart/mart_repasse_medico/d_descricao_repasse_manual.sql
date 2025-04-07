{{

    config( materialized = 'incremental',
            unique_key = '"CD_REPASSE"' )

}}

WITH source_stg_repasse_prestador
    AS (
        SELECT
            sis."CD_REPASSE",
            sis."DS_REPASSE"
        FROM {{ ref( 'stg_repasse_prestador' ) }} sis
        {% if is_incremental() %}
        WHERE sis."CD_REPASSE" > ( SELECT MAX("CD_REPASSE") FROM {{ this }} )
        {% endif %}
),
treats
    AS (
        SELECT * FROM source_stg_repasse_prestador
)
SELECT * FROM treats