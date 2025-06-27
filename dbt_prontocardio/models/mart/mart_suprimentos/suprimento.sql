{{
    config( materialized = 'incremental',
            incremental_strategy = 'merge',
            unique_key = '"CD_SUPRIMENTO_KEY"',
            merge_update_columns = ['"TP_SITUACAO_SC"', '"TP_SITUACAO_OC"', '"CD_MOT_CANCEL_SC"', '"CD_MOT_CANCEL_OC"', '"DT_CANCEL_SOL"', '"DT_CANCEL_ORD"'] )
}}

WITH source_int_suprimento
    AS (
        SELECT
            sis."CD_SUPRIMENTO_KEY",
            sis."CD_SOL_COM",
            sis."CD_SETOR",
            sis."CD_ESTOQUE",
            sis."CD_MOT_PED",
            sis."CD_MOT_CANCEL_SC",
            sis."DT_SOL_COM",
            sis."TP_SITUACAO_SC",
            sis."SN_APROVADA",
            sis."SN_URGENTE",
            sis."SN_OPME",
            sis."CD_ORD_COM",
            sis."DT_ORD_COM",
            sis."DT_PREV_ENTREGA",
            sis."DT_AUTORIZACAO",
            sis."CD_MOT_CANCEL_OC",
            sis."TP_SITUACAO_OC",
            sis."SN_AUTORIZADO_OC",
            sis."CD_FORNECEDOR",
            sis."NR_DOCUMENTO",
            sis."DT_ENTRADA",
            sis."CD_UNI_PRO",
            sis."CD_PRODUTO",
            sis."DT_CANCEL_SOL",
            sis."QT_SOLIC_SOL",
            sis."QT_COMPRADA_SOL",
            sis."QT_ATENDIDA_SOL",
            sis."DT_CANCEL_ORD",
            sis."QT_COMPRADA_ORD",
            sis."QT_ATENDIDA_ORD",
            sis."QT_RECEBIDA_ORD",
            sis."QT_CANCELADA_ORD",
            sis."VL_UNITARIO_ORD",
            sis."SALDO",
            sis."QT_ENTRADA_ENT",
            sis."QT_ATENDIDA_ENT",
            sis."QT_UNITARIO_ENT",
            sis."QT_ESTOQUE_ATUAL",
            sis."QT_CONSUMO_ATUAL",
            sis."DT_ULTIMA_MOVIMENTACAO",
            sis."MATCHING",
            sis."QT_MOVIMENTO"
        FROM {{ ref( 'int_suprimento' ) }} sis
        {% if is_incremental() %}
        WHERE (
                sis."DT_SOL_COM" >= (current_date - interval '30 day')
                OR
                sis."CD_SUPRIMENTO_KEY" > ( SELECT MAX("CD_SUPRIMENTO_KEY") FROM {{this}} )
            )
        {% endif %}
),
mrt_suprimento
    AS (
        SELECT
            *
        FROM source_int_suprimento
)
SELECT * FROM mrt_suprimento