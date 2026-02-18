{{
    config(
        tags = ['entradas']
    )
}}

WITH source_item_previsao
    AS (
        SELECT
            cd_itcon_pag,
            cd_con_pag,
            nr_parcela,
            dt_vencimento,
            dt_prevista_pag,
            tp_quitacao,
            vl_duplicata
        FROM {{ ref('stg_itcon_pag') }}
),
source_previsao
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
        FROM {{ ref('stg_con_pag') }}
),
source_pagamentos
    AS (
        SELECT
            cd_pagcon_pag,
            cd_itcon_pag,
            ds_pagcon_pag,
            dt_pagamento,
            dt_baixa,
            tp_pagamento,
            vl_acrescimo,
            vl_desconto,
            vl_pago
        FROM {{ ref('stg_pagcon_pag') }}
),
source_setor
    AS (
        SELECT
            cd_con_pag,
            cd_setor
        FROM {{ ref('stg_ratcon_pag') }}
),
treats
    AS (
        SELECT
            sip.cd_itcon_pag,
            sp.cd_con_pag,
            spg.cd_pagcon_pag,
            s.cd_setor,
            sp.cd_fornecedor,
            sp.nr_documento,
            sp.nr_serie,
            sp.cd_processo,
            sp.cd_reduzido,
            sp.cd_tip_doc,
            sp.ds_con_pag,
            spg.ds_pagcon_pag,
            sp.dt_lancamento,
            sip.dt_vencimento,
            sip.dt_prevista_pag,
            spg.dt_pagamento,
            sip.tp_quitacao,
            sp.tp_vencimento,
            spg.tp_pagamento,
            sip.nr_parcela,
            sip.vl_duplicata,
            sp.vl_bruto_conta,
            spg.vl_acrescimo,
            spg.vl_desconto,
            spg.vl_pago
        FROM source_item_previsao sip
        LEFT JOIN source_previsao sp    ON sip.cd_con_pag = sp.cd_con_pag
        LEFT JOIN source_pagamentos spg ON sip.cd_itcon_pag = spg.cd_itcon_pag
        LEFT JOIN source_setor s        ON sp.cd_con_pag = s.cd_con_pag
),
chave_substituta
    AS (
        SELECT
            {{ dbt_utils.generate_surrogate_key(['cd_fornecedor', 'nr_documento']) }} AS sk_fornecedor_documento,
            *
        FROM treats
)
SELECT * FROM chave_substituta
