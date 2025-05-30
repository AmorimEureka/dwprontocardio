SELECT
    rc.CD_PRO_FAT,
    rc.CD_REG_FAT,
    rc.CD_PRESTADOR_REPASSE,
    rc.CD_ATI_MED,
    rc.CD_LANC_FAT,
    rc.CD_GRU_FAT,
    rc.CD_GRU_PRO,
    rc.CD_PROCEDIMENTO,
    rc.DT_LANCAMENTO AS DT_REPASSE_CONSOLIDADO,
    rc.DT_COMPETENCIA_FAT,
    rc.DT_COMPETENCIA_REP,
    rc.SN_PERTENCE_PACOTE,
    rc.VL_SP,
    rc.VL_ATO,
    rc.VL_REPASSE,
    rc.VL_TOTAL_CONTA,
    rc.VL_BASE_REPASSADO
FROM DBAMV.REPASSE_CONSOLIDADO rc
WHERE TRUNC(rc.DT_LANCAMENTO) >= ADD_MONTHS(TRUNC(TO_DATE( ':PARAM' , 'YYYY-MM-DD')), -6)