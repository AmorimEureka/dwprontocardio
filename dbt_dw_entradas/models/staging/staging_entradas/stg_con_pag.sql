{{
    config(
        materialized = 'incremental',
        unique_key = 'cd_con_pag',
        on_schema_change = 'sync_all_columns',
        tags = ['entradas']
    )
}}

WITH source_con_pag
    AS (
        SELECT
            cd_con_pag,
            cd_processo,
            cd_reduzido,
            cd_tip_doc,
            cd_fornecedor,
            ds_con_pag,
            nr_documento,
            nr_serie,
            dt_lancamento,
            tp_vencimento,
            vl_bruto_conta
        FROM {{ source('raw_entradas_mv', 'con_pag') }}
),
treats
    AS (
        SELECT
            cd_con_pag::BIGINT,
            cd_processo::BIGINT,
            cd_reduzido::BIGINT,
            cd_tip_doc::BIGINT,
            cd_fornecedor::BIGINT,
            ds_con_pag::VARCHAR(180),
            nr_documento::VARCHAR(17),
            nr_serie::VARCHAR(10),
            dt_lancamento::DATE,
            tp_vencimento::VARCHAR(1),
            vl_bruto_conta::NUMERIC(12,2)
        FROM source_con_pag
)
SELECT * FROM treats