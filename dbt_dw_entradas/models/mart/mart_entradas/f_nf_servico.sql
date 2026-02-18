{{
    config(
        materialized = 'incremental',
        unique_key = 'cd_ent_serv',
        on_schema_change = 'sync_all_columns',
        tags = ['entradas']
    )
}}

SELECT
    cd_ent_serv,
    sk_fornecedor_documento,
    cd_con_pag,
    cd_fornecedor,
    nr_documento,
    nr_serie,
    cd_oficina,
    cd_setor,
    cd_tip_doc,
    cd_usuario,
    dt_entrada,
    dt_emissao,
    ds_observacao,
    vl_total
FROM {{ ref('int_notas_servicos') }}

