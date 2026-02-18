{{
    config(
        materialized = 'incremental',
        unique_key = 'cd_ratcon_pag',
        on_schema_change = 'sync_all_columns',
        tags = ['entradas']
    )
}}

WITH source_ratcon_pag
    AS (
        SELECT
            cd_ratcon_pag,
            cd_con_pag,
            cd_setor,
            cd_reduzido,
            cd_item_res,
            dt_competencia,
            vl_rateio
        FROM {{ source('raw_entradas_mv', 'ratcon_pag') }}
),
treats
    AS (
        SELECT
            cd_ratcon_pag::BIGINT,
            cd_con_pag::BIGINT,
            cd_setor::BIGINT,
            cd_reduzido::BIGINT,
            cd_item_res::BIGINT,
            dt_competencia::DATE,
            vl_rateio::NUMERIC(12,2)
        FROM source_ratcon_pag
)
SELECT * FROM treats
