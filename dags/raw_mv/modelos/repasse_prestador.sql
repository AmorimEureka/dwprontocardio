

SELECT
    rp.CD_REPASSE,
    rp.CD_PRESTADOR_REPASSE,
    r.DS_REPASSE,
    r.DT_COMPETENCIA,
    r.DT_REPASSE,
    r.TP_REPASSE,
    rp.VL_REPASSE
FROM DBAMV.REPASSE r
INNER JOIN DBAMV.REPASSE_PRESTADOR rp ON r.CD_REPASSE = rp.CD_REPASSE
WHERE r.TP_REPASSE = 'M' AND rp.CD_REPASSE >= :PARAM