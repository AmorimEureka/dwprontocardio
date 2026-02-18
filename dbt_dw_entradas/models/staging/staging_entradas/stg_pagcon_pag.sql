{{
    config(
        materialized = 'incremental',
        unique_key = 'cd_pagcon_pag',
        on_schema_change = 'sync_all_columns',
        tags = ['entradas']
    )
}}

WITH source_pagcon_pag
    AS  (
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
        FROM {{ source('raw_entradas_mv', 'pagcon_pag') }}
),
treats
    AS (
        SELECT
            cd_pagcon_pag::BIGINT,
            cd_itcon_pag::BIGINT,
            ds_pagcon_pag::VARCHAR,
            dt_pagamento::DATE,
            dt_baixa::DATE,
            tp_pagamento::VARCHAR(1),
            vl_acrescimo::NUMERIC(12,2),
            vl_desconto::NUMERIC(12,2),
            vl_pago::NUMERIC(12,2)
        FROM source_pagcon_pag
)
SELECT * FROM treats
