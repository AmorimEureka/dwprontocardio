{{
    config(
        materialized = 'incremental',
        unique_key = 'cd_itcon_pag',
        on_schema_change = 'sync_all_columns',
        tags = ['entradas']
    )
}}

WITH source_itcon_pag
    AS (
        SELECT
            cd_itcon_pag,
            cd_con_pag,
            nr_parcela,
            dt_vencimento,
            dt_prevista_pag,
            tp_quitacao,
            vl_duplicata
        FROM {{ source('raw_entradas_mv', 'itcon_pag') }}
),
treats
    AS (
        SELECT
            cd_itcon_pag::BIGINT,
            cd_con_pag::BIGINT,
            nr_parcela::VARCHAR(2),
            dt_vencimento::DATE,
            dt_prevista_pag::DATE,
            tp_quitacao::VARCHAR(1),
            vl_duplicata::NUMERIC(12,2)
        FROM source_itcon_pag
)
SELECT * FROM treats

