{{
    config(
        tags = ['entradas']
    )
}}

WITH source_ent_serv
    AS (
        SELECT
            cd_ent_serv,
            cd_con_pag,
            cd_fornecedor,
            nr_documento,
            nr_serie,
            cd_oficina,
            cd_tip_doc,
            cd_usuario,
            dt_entrada,
            dt_emissao,
            ds_observacao,
            vl_total
        FROM {{ ref('stg_ent_serv') }}
),
-- source_setor
--     AS (
--         SELECT
--             cd_con_pag,
--             cd_setor
--         FROM {{ ref('stg_ratcon_pag') }}
-- ),
source_ocifina
    AS (
        SELECT
            cd_oficina,
            cd_setor
        FROM {{ ref('stg_oficina') }}
),
treats
    AS (
        SELECT
            es.cd_ent_serv,
            es.cd_con_pag,
            es.cd_fornecedor,
            es.nr_documento,
            es.nr_serie,
            es.cd_oficina,
            o.cd_setor,
            es.cd_tip_doc,
            es.cd_usuario,
            es.dt_entrada,
            es.dt_emissao,
            es.ds_observacao,
            es.vl_total
        FROM source_ent_serv es
        LEFT JOIN source_ocifina o ON es.cd_oficina = o.cd_oficina

),
chave_substituta
     AS (
        SELECT
            {{ dbt_utils.generate_surrogate_key(['cd_fornecedor', 'nr_documento']) }} AS sk_fornecedor_documento,
            *
        FROM treats
)
SELECT * FROM chave_substituta