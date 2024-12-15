

WITH parametros AS (
    SELECT
        '2023-01-01'::DATE AS data_inicio, -- Data inicial
        '2030-12-31'::DATE AS data_fim    -- Data final
),
calendario_base AS (
    SELECT
        data_inicio + n * INTERVAL '1 day' AS data_calendario
    FROM 
        parametros,
        generate_series(0, (data_fim - data_inicio)) AS n -- Subtração de datas gera o número de dias
),
atributos_calendario AS (
    SELECT
        data_calendario,
        EXTRACT(YEAR FROM data_calendario) AS ano,
        EXTRACT(MONTH FROM data_calendario) AS mes,
        EXTRACT(DAY FROM data_calendario) AS dia,
        EXTRACT(DOW FROM data_calendario) AS dia_da_semana, -- 0 (domingo) a 6 (sábado)
        CASE
            WHEN EXTRACT(DOW FROM data_calendario) IN (6, 0) THEN TRUE
            ELSE FALSE
        END AS fim_de_semana,
        EXTRACT(WEEK FROM data_calendario) AS semana_do_ano,
        TO_CHAR(data_calendario, 'Month') AS nome_mes,
        TO_CHAR(data_calendario, 'Dy') AS nome_dia,
        data_calendario = current_date AS eh_hoje,
        DATE_PART('quarter', data_calendario) AS trimestre
    FROM 
        calendario_base
)
SELECT * 
FROM atributos_calendario
ORDER BY data_calendario