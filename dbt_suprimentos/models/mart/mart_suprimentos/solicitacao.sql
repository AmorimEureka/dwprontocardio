{{
    config( materialized = 'incremental',
            unique_key = '"CD_ITSOL_COM_KEY"' )

}}



WITH source_int_solicitacao
    AS (
        SELECT
            *
        FROM {{ ref( 'int_suprimento_solicitacao' ) }} sis
            {% if is_incremental() %}
                WHERE sis."CD_ITSOL_COM_KEY" > (SELECT MAX("CD_ITSOL_COM_KEY") FROM {{ this }})
            {% endif %}
),
mrt_solicitacao
    AS (
        SELECT
            *
        FROM source_int_solicitacao
)
SELECT * FROM mrt_solicitacao
