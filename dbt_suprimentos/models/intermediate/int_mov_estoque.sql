

{{
    config( materialized = 'ephemeral' )

}}


WITH source_mov_estoque 
    AS (
        SELECT 
            "CD_MVTO_ESTOQUE"
            , "CD_ESTOQUE"
            , "CD_UNI_PRO"
            , "CD_UNID_INT"
            , "CD_SETOR"
            , "CD_ESTOQUE_DESTINO"
            , "CD_CUSTO_MEDIO"
            , "CD_AVISO_CIRURGIA"
            , "CD_ENT_PRO"
            , "CD_USUARIO"
            , "CD_FORNECEDOR"
            , "CD_PRESTADOR"
            , "CD_PRE_MED"
            , "CD_ATENDIMENTO"
            , "CD_MOT_DEV"
            , "DT_MVTO_ESTOQUE"
            , "HR_MVTO_ESTOQUE"
            , "VL_TOTAL"
            , "TP_MVTO_ESTOQUE"
            , "NR_DOCUMENTO"
            , "CHAVE_NFE"
        FROM {{ ref( 'stg_mvto_estoque' ) }}
),
source_itens_mov_estoque
    AS (
        SELECT 
            "CD_ITMVTO_ESTOQUE"
            , "CD_MVTO_ESTOQUE"
            , "CD_PRODUTO"
            , "CD_UNI_PRO"
            , "CD_LOTE"
            , "CD_ITENT_PRO"
            , "CD_FORNECEDOR"
            , "CD_ITPRE_MED"
            , "DT_VALIDADE"
            , "DH_MVTO_ESTOQUE"
            , "QT_MOVIMENTACAO"
            , "VL_UNITARIO"
            , "TP_ESTOQUE"
        FROM {{ ref( 'stg_itmvto_estoque' ) }}
        
),
source_uni_pro
    AS (
        SELECT 
            "CD_UNI_PRO"
            , "CD_UNIDADE"
            , "CD_PRODUTO"
            , "VL_FATOR"
            , "TP_RELATORIOS"
            , "SN_ATIVO"
        FROM {{ ref( 'stg_uni_pro' ) }}
        WHERE "TP_RELATORIOS" = 'G'
        
),
source_est_pro
    AS (
        SELECT 
            "CD_ESTOQUE"
            , "CD_PRODUTO"
            , "CD_LOCALIZACAO"
            , "DS_LOCALIZACAO_PRATELEIRA"
            , "DT_ULTIMA_MOVIMENTACAO"
            , "QT_ESTOQUE_ATUAL"
            , "QT_ESTOQUE_MAXIMO"
            , "QT_ESTOQUE_MINIMO"
            , "QT_ESTOQUE_VIRTUAL"
            , "QT_PONTO_DE_PEDIDO"
            , "QT_CONSUMO_MES"
            , "QT_SOLICITACAO_DE_COMPRA"
            , "QT_ORDEM_DE_COMPRA"
            , "QT_ESTOQUE_DOADO"
            , "QT_ESTOQUE_RESERVADO"
            , "QT_CONSUMO_ATUAL"
            , "TP_CLASSIFICACAO_ABC"
        FROM {{ ref( 'stg_est_pro' ) }}
        
),
treats_qt_mov
    AS (
        SELECT 
            itmve."CD_PRODUTO"
            , up."VL_FATOR"
            , up."TP_RELATORIOS"
            , mve."TP_MVTO_ESTOQUE"
            ,  SUM(itmve."QT_MOVIMENTACAO" * up."VL_FATOR" * CASE WHEN mve."TP_MVTO_ESTOQUE" IN ('D', 'C') THEN -1 ELSE 1 END )  / up."VL_FATOR"  AS "QT_MOVIMENTO"
        FROM source_itens_mov_estoque itmve
        LEFT JOIN source_mov_estoque  mve ON itmve."CD_MVTO_ESTOQUE" = mve."CD_MVTO_ESTOQUE"
        LEFT JOIN source_uni_pro      up  ON itmve."CD_UNI_PRO" = up."CD_UNI_PRO"
        GROUP BY itmve."CD_PRODUTO", up."VL_FATOR", up."TP_RELATORIOS", mve."TP_MVTO_ESTOQUE"
),
treats 
    AS (
        SELECT
            ep."CD_ESTOQUE"
            , ep."CD_PRODUTO"
            , ep."DT_ULTIMA_MOVIMENTACAO"
            , ep."QT_ESTOQUE_ATUAL"
            , ep."QT_ESTOQUE_MAXIMO"
            , ep."QT_ESTOQUE_MINIMO"
            , ep."QT_ESTOQUE_VIRTUAL"
            , ep."QT_PONTO_DE_PEDIDO"
            , ep."QT_CONSUMO_MES"
            , ep."QT_SOLICITACAO_DE_COMPRA"
            , ep."QT_ORDEM_DE_COMPRA"
            , ep."QT_ESTOQUE_DOADO"
            , ep."QT_ESTOQUE_RESERVADO"
            , ep."QT_CONSUMO_ATUAL"
            , qmv."QT_MOVIMENTO"
            , ep."TP_CLASSIFICACAO_ABC"
            , qmv."TP_MVTO_ESTOQUE"
        FROM source_est_pro ep
        LEFT JOIN treats_qt_mov qmv ON ep."CD_PRODUTO" = qmv."CD_PRODUTO"

)
SELECT * FROM treats