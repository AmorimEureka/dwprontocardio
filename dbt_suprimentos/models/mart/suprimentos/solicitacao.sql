{{
    config( materialized = 'incremental' )

}}



WITH source_int_solicitacao 
    AS (
        SELECT
            *
        FROM {{ ref( 'int_solicitacao' ) }} sis
            {% if is_incremental() %}
                WHERE sis."CD_SOL_COM" > (SELECT MAX("CD_SOL_COM") FROM {{ this }}) -- Carregar apenas novos dados
            {% endif %}
),
mrt_solicitacao
    AS (
        SELECT
            *
        FROM source_int_solicitacao
)
SELECT * FROM mrt_solicitacao
