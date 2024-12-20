SELECT 
    itpr.CD_ITENT_PRO
    , itpr.CD_ENT_PRO
    , itpr.CD_PRODUTO
    , itpr.CD_UNI_PRO
    , itpr.CD_ATENDIMENTO
    , itpr.CD_CUSTO_MEDIO
    , itpr.CD_PRODUTO_FORNECEDOR
    , itpr.DT_GRAVACAO
    , itpr.QT_ENTRADA
    , itpr.QT_DEVOLVIDA
    , itpr.QT_ATENDIDA
    , itpr.VL_UNITARIO
    , itpr.VL_CUSTO_REAL
    , itpr.VL_TOTAL_CUSTO_REAL
    , itpr.VL_TOTAL
FROM DBAMV.ITENT_PRO itpr
WHERE itpr.CD_ITENT_PRO >= :MAIOR_ID
