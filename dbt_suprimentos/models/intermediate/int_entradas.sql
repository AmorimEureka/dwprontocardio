


WITH source_entradas 
    AS (
        SELECT 
            "CD_ENT_PRO"
            , "CD_TIP_ENT"
            , "CD_ESTOQUE"
            , "CD_FORNECEDOR"
            , "CD_ORD_COM"
            , "CD_USUARIO_RECEBIMENTO"
            , "CD_ATENDIMENTO"
            , "DT_EMISSAO"
            , "DT_ENTRADA"
            , "DT_RECEBIMENTO"
            , "HR_ENTRADA"
            , "VL_TOTAL"
            , "NR_DOCUMENTO"
            , "NR_CHAVE_ACESSO"
            , "SN_AUTORIZADO"
        FROM {{ ref( 'stg_ent_pro' ) }}
),
source_itens_entradas
    AS (
        SELECT 
            "CD_ITENT_PRO"
            , "CD_ENT_PRO"
            , "CD_PRODUTO"
            , "CD_UNI_PRO"
            , "CD_ATENDIMENTO"
            , "CD_CUSTO_MEDIO"
            , "CD_PRODUTO_FORNECEDOR"
            , "DT_GRAVACAO"
            , "QT_ENTRADA"
            , "QT_DEVOLVIDA"
            , "QT_ATENDIDA"
            , "VL_UNITARIO"
            , "VL_CUSTO_REAL"
            , "VL_TOTAL_CUSTO_REAL"
            , "VL_TOTAL"
        FROM {{ ref( 'stg_itent_pro' ) }}
        
),
treats
    AS (
        SELECT 
            e."CD_ENT_PRO"
            , ite."CD_ITENT_PRO"
            , e."CD_TIP_ENT"
            , e."CD_ESTOQUE"
            , e."CD_FORNECEDOR"
            , ite."CD_PRODUTO"
            , ite."CD_UNI_PRO"
            , ite."CD_CUSTO_MEDIO"
            , e."CD_ORD_COM"
            , e."CD_USUARIO_RECEBIMENTO"
            , e."CD_ATENDIMENTO"
            , e."DT_EMISSAO"
            , e."DT_ENTRADA"
            , e."DT_RECEBIMENTO"
            , e."HR_ENTRADA"
            , ite."QT_ENTRADA"
            , ite."QT_DEVOLVIDA"
            , ite."QT_ATENDIDA"
            , ite."VL_UNITARIO"
            , ite."VL_CUSTO_REAL"
            , ite."VL_TOTAL_CUSTO_REAL"
            , ite."VL_TOTAL"
            , e."NR_DOCUMENTO"
            , e."NR_CHAVE_ACESSO"
            , e."SN_AUTORIZADO"
        FROM source_itens_entradas ite
        LEFT JOIN source_entradas e ON ite."CD_ENT_PRO" = e."CD_ENT_PRO"
)
SELECT * FROM treats