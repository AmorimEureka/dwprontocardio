SELECT
    e.CD_ESTOQUE
    , e.CD_SETOR
    , e.DS_ESTOQUE
    , e.TP_ESTOQUE
FROM DBAMV.ESTOQUE e
WHERE e.CD_ESTOQUE >= :PARAM
