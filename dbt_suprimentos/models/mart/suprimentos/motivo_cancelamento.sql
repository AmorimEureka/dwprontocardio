

{{
    config( materialized = 'incremental' )

}}



WITH source_int_motivo_cancelamento
    AS (
        SELECT
            *
        FROM {{ ref( 'int_motivo_cancelamento' ) }} sis
            {% if is_incremental() %}
                WHERE sis."CD_MOT_CANCEL" > (SELECT MAX("CD_MOT_CANCEL") FROM {{ this }}) -- Carregar apenas novos dados
            {% endif %}
),
mrt_motivo_cancelamento
    AS (
        SELECT
            *
        FROM source_int_motivo_cancelamento
)
SELECT * FROM mrt_motivo_cancelamento
