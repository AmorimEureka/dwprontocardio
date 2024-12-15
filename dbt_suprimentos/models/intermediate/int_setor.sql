

WITH source_setor
    AS (
        SELECT 
            "CD_SETOR"
            , "CD_GRUPO_DE_CUSTO"
            , "CD_SETOR_CUSTO"
            , "NM_SETOR"
            , "SN_ATIVO"
        FROM {{ ref( 'stg_setor' ) }}
),
treats
    AS (
        SELECT 
            *
        FROM source_setor

)
SELECT * FROM treats