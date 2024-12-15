

{{
    config( materialized = 'incremental' )

}}



WITH source_int_fornecedor
    AS (
        SELECT
            *
        FROM {{ ref( 'int_fornecedor' ) }} sis
            {% if is_incremental() %}
                WHERE sis."CD_FORNECEDOR" > (SELECT MAX("CD_FORNECEDOR") FROM {{ this }}) -- Carregar apenas novos dados
            {% endif %}
),
mrt_fornecedor
    AS (
        SELECT DISTINCT
            *
        FROM source_int_fornecedor
)
SELECT * FROM mrt_fornecedor
