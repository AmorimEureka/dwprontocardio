

{% snapshot snp_ord_com %}

    {{
        config(
            target_database = 'dwprontocardio',
            target_schema = 'snapshots',
            unique_key='"CD_ORD_COM"',
            strategy= 'check',
            check_cols=['"TP_SITUACAO"'],
        )
    }}

    SELECT
        "CD_ORD_COM"::BIGINT
        , "CD_ESTOQUE"::BIGINT
        , "CD_FORNECEDOR"::BIGINT
        , "CD_SOL_COM"::BIGINT
        , "CD_MOT_CANCEL"::BIGINT
        , "CD_USUARIO_CRIADOR_OC"::VARCHAR(30)
        , "CD_ULTIMO_USU_ALT_OC"::VARCHAR(30)
        , "DT_ORD_COM"::DATE
        , "DT_PREV_ENTREGA"::DATE
        , "DT_CANCELAMENTO"::TIMESTAMP
        , "DT_AUTORIZACAO"::TIMESTAMP
        , "DT_ULTIMA_ALTERACAO_OC"::DATE
        , "TP_SITUACAO"::VARCHAR(1)
        , "TP_ORD_COM"::VARCHAR(1)
        , "SN_AUTORIZADO"::VARCHAR(1)
        , "DT_EXTRACAO"::TIMESTAMP
    FROM {{ ref('stg_ord_com') }}

{% endsnapshot %}