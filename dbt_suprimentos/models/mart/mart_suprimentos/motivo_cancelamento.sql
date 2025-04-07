

{{
    config( materialized = 'incremental',
            unique_key = '"CD_MOT_CANCEL"' )

}}



WITH source_int_motivo_cancelamento
    AS (
        SELECT
            *
        FROM {{ ref( 'int_suprimento_motivo_cancelamento' ) }} sis
        {% if is_incremental() %}
        WHERE sis."CD_MOT_CANCEL" > (SELECT MAX("CD_MOT_CANCEL") FROM {{ this }})
        {% endif %}
),
mrt_motivo_cancelamento
    AS (
        SELECT
            *
        FROM source_int_motivo_cancelamento
)
SELECT * FROM mrt_motivo_cancelamento
