

WITH source_fornecedor 
    AS (
        SELECT 
            "CD_FORNECEDOR"
            , "NM_FORNECEDOR"
            , "NM_FANTASIA"
            , "DT_INCLUSAO"
            , "NR_CGC_CPF"
            , "TP_FORNECEDOR"
        FROM {{ ref( 'stg_fornecedor' ) }}
),
treats
    AS (
        SELECT 
            *
        FROM source_fornecedor

)
SELECT * FROM treats