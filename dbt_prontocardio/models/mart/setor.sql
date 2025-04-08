
{{
    config( materialized = 'incremental' )

}}


WITH source_int_setor
    AS (
        SELECT
            sis."CD_SETOR"
            , sis."CD_GRUPO_DE_CUSTO"
            , sis."CD_SETOR_CUSTO"
            , sis."NM_SETOR"
            , sis."SN_ATIVO"
        FROM {{ ref( 'int_setor' ) }} sis
            {% if is_incremental() %}
                WHERE sis."CD_SETOR" > (SELECT MAX("CD_SETOR") FROM {{ this }})
            {% endif %}
),
treats
    AS (
        SELECT
            *
        FROM source_int_setor

)
SELECT * FROM treats