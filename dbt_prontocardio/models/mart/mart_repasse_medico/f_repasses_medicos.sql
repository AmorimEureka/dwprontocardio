{{

    config( materialized = 'incremental',
            unique_key = '"CD_ITREG_KEY"',
            merge_update_columns = ['"CD_REMESSA"', '"DT_REMESSA"', '"SN_FECHADA"', '"DT_FECHAMENTO"'] )

}}

WITH source_int_repasses_medicos
    AS (
        SELECT
            *
        FROM {{ ref('int_repasses_medicos') }} sis
        {% if is_incremental() %}
        WHERE sis."CD_ITREG_KEY" > ( SELECT MAX("CD_ITREG_KEY") FROM {{this}} )
        {% endif %}
),
mrt_repasses_medicos
    AS (
        SELECT
            *
        FROM source_int_repasses_medicos
)
SELECT * FROM mrt_repasses_medicos