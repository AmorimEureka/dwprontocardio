{{
    config(
        materialized = 'incremental',
        unique_key = 'cd_itcon_pag',
        on_schema_change = 'sync_all_columns',
        tags = ['entradas']
    )
}}

SELECT
    cd_itcon_pag,
    sk_fornecedor_documento,
    cd_con_pag,
    cd_pagcon_pag,
    cd_setor,
    cd_fornecedor,
    nr_documento,
    nr_serie,
    cd_processo,
    cd_reduzido,
    cd_tip_doc,
    ds_con_pag,
    ds_pagcon_pag,
    dt_lancamento,
    dt_vencimento,
    dt_prevista_pag,
    dt_pagamento,
    tp_quitacao,
    tp_vencimento,
    tp_pagamento,
    nr_parcela,
    vl_duplicata,
    vl_bruto_conta,
    vl_acrescimo,
    vl_desconto,
    vl_pago
FROM {{ ref('int_contas_a_pagar') }}
