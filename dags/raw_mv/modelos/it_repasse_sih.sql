SELECT
    sih.CD_IT_REPASSE_SIH,
    sih.CD_REPASSE,
    sih.CD_REG_FAT,
    sih.CD_LANCAMENTO,
    sih.CD_ATI_MED,
    sih.CD_PRESTADOR_REPASSE,
    sih.VL_REPASSE
FROM DBAMV.IT_REPASSE_SIH sih
WHERE sih.CD_REPASSE >= :PARAM