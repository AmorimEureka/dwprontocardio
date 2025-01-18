
SELECT 
    mvt.CD_MVTO_ESTOQUE
    , mvt.CD_ESTOQUE
    , mvt.CD_UNI_PRO
    , mvt.CD_UNID_INT
    , mvt.CD_SETOR
    , mvt.CD_ESTOQUE_DESTINO
    , mvt.CD_CUSTO_MEDIO
    , mvt.CD_AVISO_CIRURGIA
    , mvt.CD_ENT_PRO
    , mvt.CD_USUARIO
    , mvt.CD_FORNECEDOR
    , mvt.CD_PRESTADOR
    , mvt.CD_PRE_MED
    , mvt.CD_ATENDIMENTO
    , mvt.CD_MOT_DEV
    , mvt.DT_MVTO_ESTOQUE
    , mvt.HR_MVTO_ESTOQUE
    , mvt.VL_TOTAL
    , mvt.TP_MVTO_ESTOQUE
    , mvt.NR_DOCUMENTO
    , mvt.CHAVE_NFE
FROM DBAMV.MVTO_ESTOQUE mvt
WHERE mvt.CD_MVTO_ESTOQUE >= :PARAM