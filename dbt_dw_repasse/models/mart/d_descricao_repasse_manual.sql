{{

    config( materialized = 'incremental',
            unique_key = 'cd_repasse',
            on_schema_change = 'sync_all_columns',
            tags = ['repasse']
    )
}}

WITH source_stg_repasse_prestador
    AS (
        SELECT
            sis.cd_repasse,
            sis.ds_repasse
        FROM {{ ref( 'stg_repasse_prestador' ) }} sis
        {% if is_incremental() %}
        WHERE sis.cd_repasse > ( SELECT MAX(cd_repasse) FROM {{ this }} )
        {% endif %}
),
treats
    AS (
        SELECT * FROM source_stg_repasse_prestador
)
SELECT * FROM treats