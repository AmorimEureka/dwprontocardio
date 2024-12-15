
WITH source_pedidos 
    AS (
        SELECT 
            "CD_ORD_COM"
            , "CD_ESTOQUE"
            , "CD_FORNECEDOR"
            , "CD_SOL_COM"
            , "CD_MOT_CANCEL"
            , "CD_USUARIO_CRIADOR_OC"
            , "CD_ULTIMO_USU_ALT_OC"
            , "DT_ORD_COM"
            , "DT_CANCELAMENTO"
            , "DT_AUTORIZACAO"
            , "DT_ULTIMA_ALTERACAO_OC"
            , CASE 
                WHEN "TP_ORD_COM" = 'A' THEN 'Aberta'
                WHEN "TP_ORD_COM" = 'U' THEN 'Autorizada'
                WHEN "TP_ORD_COM" = 'N' THEN 'Não Autorizada'
                WHEN "TP_ORD_COM" = 'P' THEN 'Pendente'
                WHEN "TP_ORD_COM" = 'L' THEN 'Parcialmente Atendida'
                WHEN "TP_ORD_COM" = 'T' THEN 'Atendida'
                WHEN "TP_ORD_COM" = 'C' THEN 'Cancelada'
                WHEN "TP_ORD_COM" = 'D' THEN 'Adjudicação'
                WHEN "TP_ORD_COM" = 'O' THEN 'Aguard. Próximo Nível'
              END AS "TP_ORD_COM"
            , "SN_AUTORIZADO"
        FROM {{ ref( 'stg_ord_com' ) }}
),
source_itens_pedidos
    AS (
        SELECT 
            "CD_ORD_COM"
            , "CD_PRODUTO"
            , "CD_UNI_PRO"
            , "CD_MOT_CANCEL"
            , "DT_CANCEL"
            , "QT_COMPRADA"
            , "QT_ATENDIDA"
            , "QT_RECEBIDA"
            , "QT_CANCELADA"
            , "VL_UNITARIO"
            , "VL_TOTAL"
            , "VL_CUSTO_REAL"
            , "VL_TOTAL_CUSTO_REAL"
        FROM {{ ref( 'stg_itord_pro' ) }}
        
),
treats
    AS (
        SELECT 
            p."CD_ORD_COM"
            , p."CD_SOL_COM"
            , p."CD_ESTOQUE"
            , p."CD_FORNECEDOR"
            , itp."CD_PRODUTO"
            , itp."CD_UNI_PRO"
            , p."CD_MOT_CANCEL"
            , p."CD_USUARIO_CRIADOR_OC"
            , p."CD_ULTIMO_USU_ALT_OC"
            , p."DT_ORD_COM"
            , p."DT_AUTORIZACAO"
            , p."DT_ULTIMA_ALTERACAO_OC"
            , itp."QT_COMPRADA"
            , itp."QT_ATENDIDA"
            , itp."QT_RECEBIDA"
            , itp."QT_CANCELADA"
            , itp."VL_UNITARIO"
            , itp."VL_TOTAL"
            , itp."VL_CUSTO_REAL"
            , itp."VL_TOTAL_CUSTO_REAL"
            , p."TP_ORD_COM"
            , p."SN_AUTORIZADO" 
        FROM source_itens_pedidos itp
        LEFT JOIN source_pedidos p ON itp."CD_ORD_COM" = p."CD_ORD_COM"
)
SELECT * FROM treats