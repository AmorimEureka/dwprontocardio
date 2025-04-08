
{% snapshot snp_sol_com %}

    {{
        config(
            target_database = 'dwprontocardio',
            target_schema = 'snapshots',
            unique_key='"CD_SOL_COM"',
            strategy= 'check',
            check_cols=['"TP_SITUACAO"'],
        )
    }}

    SELECT 
        "CD_SOL_COM"::BIGINT
        , "CD_MOT_PED"::BIGINT
        , "CD_SETOR"::BIGINT
        , "CD_ESTOQUE"::BIGINT
        , "CD_MOT_CANCEL"::BIGINT
        , "CD_ATENDIME"::BIGINT
        , "CD_SOLICITANTE"::VARCHAR(50)
        , "NM_SOLICITANTE"::VARCHAR(25)
        , "DT_SOL_COM"::DATE
        , "DT_CANCELAMENTO"::TIMESTAMP
        , "VL_TOTAL"::NUMERIC(12,2)
        , "TP_SITUACAO"::VARCHAR(1)
        , "TP_SOL_COM"::VARCHAR(1)
        , "SN_URGENTE"::VARCHAR(1)
        , "SN_APROVADA"::VARCHAR(1)
        , "SN_OPME"::VARCHAR(1)
        , "DT_EXTRACAO"::TIMESTAMP
    FROM {{ ref('stg_sol_com') }}


{% endsnapshot %}