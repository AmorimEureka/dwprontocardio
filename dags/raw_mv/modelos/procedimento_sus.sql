

SELECT
    ps.CD_PROCEDIMENTO,
    ps.DS_PROCEDIMENTO
FROM DBAMV.PROCEDIMENTO_SUS ps
WHERE TO_NUMBER(TRIM(ps.CD_PROCEDIMENTO)) >= :PARAM