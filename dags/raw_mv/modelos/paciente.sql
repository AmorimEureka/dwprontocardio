SELECT
    pa.CD_PACIENTE,
    pa.NM_PACIENTE
FROM DBAMV.PACIENTE pa
WHERE pa.CD_PACIENTE >= :PARAM