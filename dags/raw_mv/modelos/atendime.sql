SELECT
    a.CD_ATENDIMENTO,
    a.CD_PACIENTE,
    a.CD_PRESTADOR,
    a.DT_ATENDIMENTO,
    a.TP_ATENDIMENTO
FROM DBAMV.ATENDIME a
WHERE a.CD_ATENDIMENTO >= :PARAM