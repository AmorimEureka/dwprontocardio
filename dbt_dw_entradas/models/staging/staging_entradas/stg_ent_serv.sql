{{
    config(
        materialized = 'incremental',
        unique_key = 'cd_ent_serv',
        on_schema_change = 'sync_all_columns',
        tags = ['entradas']
    )
}}

WITH source_ent_serv
    AS (
        SELECT
           cd_ent_serv,
           cd_con_pag,
           cd_fornecedor,
           cd_oficina,
           cd_tip_doc,
           cd_usuario,
           nr_documento,
           nr_serie,
           dt_entrada,
           dt_emissao,
           ds_observacao,
           vl_total
        FROM {{ source('raw_entradas_mv', 'ent_serv') }}
),
treats
    AS (
        SELECT
           cd_ent_serv::BIGINT,
           cd_con_pag::BIGINT,
           cd_fornecedor::BIGINT,
           cd_oficina::BIGINT,
           cd_tip_doc::BIGINT,
           cd_usuario::VARCHAR(20),
           nr_documento::VARCHAR(15),
           nr_serie::VARCHAR(10),
           dt_entrada::DATE,
           dt_emissao::DATE,
           ds_observacao::VARCHAR(2000),
           vl_total::NUMERIC(12,2)
        FROM source_ent_serv
)
SELECT * FROM treats
