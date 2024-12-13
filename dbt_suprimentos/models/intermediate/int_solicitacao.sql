
WITH source_solicitacao
    AS (
        SELECT 
            "CD_SOL_COM"
            , "CD_MOT_PED"
            , "CD_SETOR"
            , "CD_ESTOQUE"
            , "CD_MOT_CANCEL"
            , "CD_ATENDIME"
            , "NM_SOLICITANTE"
            , "DT_SOL_COM"
            , "DT_CANCELAMENTO"
            , "VL_TOTAL"
            , "TP_SITUACAO"
            , "TP_SOL_COM"
            , "SN_URGENTE"
            , "SN_APROVADA"
            , "SN_OPME"
        FROM {{ ref( 'stg_sol_com' ) }}
),
source_itens_solicitacao
    AS (
        SELECT 
            "CD_SOL_COM"
            , "CD_PRODUTO"
            , "CD_UNI_PRO"
            , "CD_MOT_CANCEL"
            , "DT_CANCEL"
            , "QT_SOLIC"
            , "QT_COMPRADA"
            , "QT_ATENDIDA"
            , "SN_COMPRADO"
        FROM {{ ref( 'stg_itsol_com' ) }}
        
),
treats
    AS (
        SELECT 
            s."CD_SOL_COM"
            , s."CD_ESTOQUE"
            , s."CD_SETOR"
            , s."CD_MOT_PED"
            , its."CD_PRODUTO"
            , its."CD_UNI_PRO"
            , s."CD_MOT_CANCEL"
            , s."CD_ATENDIME"
            , s."DT_SOL_COM"
            , s."DT_CANCELAMENTO"
            , its."QT_SOLIC"
            , its."QT_COMPRADA"
            , its."QT_ATENDIDA"
            , s."VL_TOTAL"
            , s."TP_SITUACAO"
            , s."TP_SOL_COM"
            , s."SN_URGENTE"
            , s."SN_APROVADA"
            , its."SN_COMPRADO"
            , s."SN_OPME"
        FROM source_itens_solicitacao its
        LEFT JOIN source_solicitacao s ON its."CD_SOL_COM" = s."CD_SOL_COM"
)
SELECT * FROM treats