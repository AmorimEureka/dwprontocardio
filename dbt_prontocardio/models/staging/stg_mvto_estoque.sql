{{
    config( materialized = 'incremental',
            unique_key = '"CD_MVTO_ESTOQUE"' )
}}

WITH source_mvto_estoque
    AS (
        SELECT
            NULLIF(sis."CD_MVTO_ESTOQUE", 'NaN') AS "CD_MVTO_ESTOQUE"
            , NULLIF(sis."CD_ESTOQUE", 'NaN') AS "CD_ESTOQUE"
            , NULLIF(sis."CD_UNI_PRO", 'NaN') AS "CD_UNI_PRO"
            , NULLIF(SPLIT_PART(sis."CD_UNID_INT", '.', 1), 'NaN') AS "CD_UNID_INT"
            , NULLIF(SPLIT_PART(sis."CD_SETOR", '.', 1), 'NaN') AS "CD_SETOR"
            , NULLIF(SPLIT_PART(sis."CD_ESTOQUE_DESTINO", '.', 1), 'NaN') AS "CD_ESTOQUE_DESTINO"
            , NULLIF(sis."CD_CUSTO_MEDIO", 'NaN') AS "CD_CUSTO_MEDIO"
            , NULLIF(SPLIT_PART(sis."CD_AVISO_CIRURGIA", '.', 1), 'NaN') AS "CD_AVISO_CIRURGIA"
            , NULLIF(SPLIT_PART(sis."CD_ENT_PRO", '.', 1), 'NaN') AS "CD_ENT_PRO"
            , NULLIF(sis."CD_USUARIO", 'NaN') AS "CD_USUARIO"
            , NULLIF(SPLIT_PART(sis."CD_FORNECEDOR", '.', 1), 'NaN') AS "CD_FORNECEDOR"
            , NULLIF(SPLIT_PART(sis."CD_PRESTADOR", '.', 1), 'NaN') AS "CD_PRESTADOR"
            , NULLIF(SPLIT_PART(sis."CD_PRE_MED", '.', 1), 'NaN') AS "CD_PRE_MED"
            , NULLIF(SPLIT_PART(sis."CD_ATENDIMENTO", '.', 1), 'NaN') AS "CD_ATENDIMENTO"
            , NULLIF(SPLIT_PART(sis."CD_MOT_DEV", '.', 1), 'NaN') AS "CD_MOT_DEV"
            , sis."DT_MVTO_ESTOQUE"
            , sis."HR_MVTO_ESTOQUE"
            , NULLIF(sis."VL_TOTAL", 'NaN') AS "VL_TOTAL"
            , NULLIF(sis."TP_MVTO_ESTOQUE", 'NaN') AS "TP_MVTO_ESTOQUE"
            , NULLIF(sis."NR_DOCUMENTO", 'NaN') AS "NR_DOCUMENTO"
            , NULLIF(sis."CHAVE_NFE", 'NaN') AS "CHAVE_NFE"
            , sis."DT_EXTRACAO"
        FROM {{ source('raw_mv' , 'mvto_estoque') }} sis
        {% if is_incremental() %}
        WHERE sis."CD_MVTO_ESTOQUE"::BIGINT > ( SELECT MAX("CD_MVTO_ESTOQUE") FROM {{this}} )
        {% endif %}
),
treats
    AS (
        SELECT
            CAST("CD_MVTO_ESTOQUE" AS BIGINT) AS "CD_MVTO_ESTOQUE"
            , CAST("CD_ESTOQUE" AS BIGINT) AS "CD_ESTOQUE"
            , CAST("CD_UNI_PRO" AS BIGINT) AS "CD_UNI_PRO"
            , CAST("CD_UNID_INT" AS BIGINT) AS "CD_UNID_INT"
            , CAST("CD_SETOR" AS BIGINT) AS "CD_SETOR"
            , CAST("CD_ESTOQUE_DESTINO" AS BIGINT) AS "CD_ESTOQUE_DESTINO"
            , CAST("CD_CUSTO_MEDIO" AS BIGINT) AS "CD_CUSTO_MEDIO"
            , CAST("CD_AVISO_CIRURGIA" AS BIGINT) AS "CD_AVISO_CIRURGIA"
            , CAST("CD_ENT_PRO" AS BIGINT) AS "CD_ENT_PRO"
            , CAST("CD_USUARIO" AS VARCHAR(50)) AS "CD_USUARIO"
            , CAST("CD_FORNECEDOR" AS BIGINT) AS "CD_FORNECEDOR"
            , CAST("CD_PRESTADOR" AS BIGINT) AS "CD_PRESTADOR"
            , CAST("CD_PRE_MED" AS BIGINT) AS "CD_PRE_MED"
            , CAST("CD_ATENDIMENTO" AS BIGINT) AS "CD_ATENDIMENTO"
            , CAST("CD_MOT_DEV" AS BIGINT) AS "CD_MOT_DEV"
            , CAST("DT_MVTO_ESTOQUE" AS DATE) AS "DT_MVTO_ESTOQUE"
            , CAST("HR_MVTO_ESTOQUE" AS TIMESTAMP) AS "HR_MVTO_ESTOQUE"
            , CAST("VL_TOTAL" AS NUMERIC(15,0)) AS "VL_TOTAL"
            , CAST("TP_MVTO_ESTOQUE" AS VARCHAR(1)) AS "TP_MVTO_ESTOQUE"
            , CAST("NR_DOCUMENTO" AS VARCHAR(20)) AS "NR_DOCUMENTO"
            , CAST("CHAVE_NFE" AS VARCHAR(44)) AS "CHAVE_NFE"
        FROM source_mvto_estoque
    )
SELECT * FROM treats