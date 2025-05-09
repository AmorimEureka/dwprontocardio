WITH source_snp_sol_com AS (
    SELECT
        sc."CD_SOL_COM",
        sc."CD_SETOR",
        sc."CD_ESTOQUE",
        sc."CD_MOT_PED",
        sc."CD_MOT_CANCEL" AS "CD_MOT_CANCEL_SC",
        sc."DT_SOL_COM",
        sc."DT_CANCELAMENTO",
        CASE
            WHEN sc."TP_SITUACAO" = 'A' THEN 'Aberta'
            WHEN sc."TP_SITUACAO" = 'F' THEN 'Fechada'
            WHEN sc."TP_SITUACAO" = 'P' THEN 'Parcialmente Atendida'
            WHEN sc."TP_SITUACAO" = 'S' THEN 'Solicitada'
            WHEN sc."TP_SITUACAO" = 'C' THEN 'Cancelada'
            WHEN sc."TP_SITUACAO" = 'N' THEN 'Lancamento'
        END AS "TP_SITUACAO_SC",
        sc."SN_APROVADA",
        sc."SN_URGENTE",
        sc."SN_OPME"
    FROM {{ ref('snp_sol_com') }} sc
    WHERE sc.dbt_valid_to IS NULL
),
source_snp_ord_com AS (
    SELECT
        oc."CD_ORD_COM",
        oc."CD_SOL_COM",
        io."CD_PRODUTO",
        oc."CD_MOT_CANCEL" AS "CD_MOT_CANCEL_OC",
        oc."DT_ORD_COM",
        oc."DT_AUTORIZACAO",
        CASE
            WHEN oc."TP_SITUACAO" = 'A' THEN 'Aberta'
            WHEN oc."TP_SITUACAO" = 'U' THEN 'Autorizada'
            WHEN oc."TP_SITUACAO" = 'N' THEN 'Não Autorizada'
            WHEN oc."TP_SITUACAO" = 'P' THEN 'Pendente'
            WHEN oc."TP_SITUACAO" = 'L' THEN 'Parcialmente Atendida'
            WHEN oc."TP_SITUACAO" = 'T' THEN 'Atendida'
            WHEN oc."TP_SITUACAO" = 'C' THEN 'Cancelada'
            WHEN oc."TP_SITUACAO" = 'D' THEN 'Adjudicação'
            WHEN oc."TP_SITUACAO" = 'O' THEN 'Aguard. Próximo Nível'
        END AS "TP_SITUACAO_OC",
        oc."SN_AUTORIZADO" AS "SN_AUTORIZADO_OC"
    FROM {{ ref('snp_ord_com') }} oc
    LEFT JOIN {{ ref('stg_itord_pro') }} io ON oc."CD_ORD_COM" = io."CD_ORD_COM"
    WHERE oc.dbt_valid_to IS NULL
),
source_sol_ord_com AS (
    SELECT
        sc."CD_SOL_COM",
        sc."CD_SETOR",
        sc."CD_ESTOQUE",
        sc."CD_MOT_PED",
        sc."CD_MOT_CANCEL_SC",
        sc."DT_SOL_COM",
        sc."DT_CANCELAMENTO",
        sc."TP_SITUACAO_SC",
        sc."SN_APROVADA",
        sc."SN_URGENTE",
        sc."SN_OPME",
        oc."CD_ORD_COM",
        oc."CD_PRODUTO",
        oc."CD_MOT_CANCEL_OC",
        oc."DT_ORD_COM",
        oc."DT_AUTORIZACAO",
        oc."TP_SITUACAO_OC",
        oc."SN_AUTORIZADO_OC"
    FROM source_snp_sol_com sc
    LEFT JOIN source_snp_ord_com oc ON sc."CD_SOL_COM" = oc."CD_SOL_COM"
),
source_itens_solicitacao AS (
    SELECT
        ic."CD_ITSOL_COM_KEY",
        ic."CD_SOL_COM",
        ic."CD_PRODUTO",
        ic."CD_UNI_PRO",
        ic."DT_CANCEL",
        ic."QT_SOLIC",
        ic."QT_COMPRADA",
        ic."QT_ATENDIDA"
    FROM {{ ref('stg_itsol_com') }} ic
),
source_itens_pedidos AS (
    SELECT
        io."CD_ITORD_PRO_KEY",
        io."CD_ORD_COM",
        io."CD_PRODUTO",
        io."CD_UNI_PRO",
        io."DT_CANCEL",
        io."QT_COMPRADA",
        io."QT_ATENDIDA",
        io."QT_RECEBIDA",
        io."QT_CANCELADA",
        io."VL_UNITARIO",
        io."VL_TOTAL"
    FROM {{ ref('stg_itord_pro') }} io
),
source_itens_entradas AS (
    SELECT
        ip."CD_ITENT_PRO",
        ip."CD_ENT_PRO",
        ep."CD_ORD_COM",
        ep."CD_FORNECEDOR",
        ep."DT_ENTRADA",
        ep."NR_DOCUMENTO",
        ip."CD_PRODUTO",
        ip."DT_GRAVACAO",
        ip."QT_ENTRADA",
        ip."QT_ATENDIDA",
        ip."VL_UNITARIO",
        ip."VL_CUSTO_REAL",
        ip."VL_TOTAL_CUSTO_REAL",
        ip."VL_TOTAL"
    FROM {{ ref('stg_itent_pro') }} ip
    LEFT JOIN {{ ref('stg_ent_pro') }} ep ON ip."CD_ENT_PRO" = ep."CD_ENT_PRO"
),
source_est_pro AS (
    SELECT
        ep."CD_ESTOQUE",
        ep."CD_PRODUTO",
        ep."DT_ULTIMA_MOVIMENTACAO",
        ep."QT_ESTOQUE_ATUAL",
        ep."QT_ESTOQUE_MAXIMO",
        ep."QT_ESTOQUE_MINIMO",
        ep."QT_ESTOQUE_VIRTUAL",
        ep."QT_PONTO_DE_PEDIDO",
        ep."QT_CONSUMO_MES",
        ep."QT_CONSUMO_ATUAL",
        ep."TP_CLASSIFICACAO_ABC"
    FROM {{ ref('stg_est_pro') }} ep
),
treats_qt_mov AS (
    SELECT
        itmve."CD_PRODUTO",
        up."VL_FATOR",
        up."TP_RELATORIOS",
        mve."TP_MVTO_ESTOQUE",
        COALESCE(
            SUM(
                itmve."QT_MOVIMENTACAO" * up."VL_FATOR" * CASE
                    WHEN mve."TP_MVTO_ESTOQUE" IN ('D', 'C') THEN -1
                    ELSE 1
                END
            ) / up."VL_FATOR",
            0
        ) AS "QT_MOVIMENTO"
    FROM {{ ref('stg_itmvto_estoque') }} itmve
    LEFT JOIN {{ ref('stg_mvto_estoque') }} mve ON itmve."CD_MVTO_ESTOQUE" = mve."CD_MVTO_ESTOQUE"
    LEFT JOIN {{ ref('stg_uni_pro') }} up ON itmve."CD_UNI_PRO" = up."CD_UNI_PRO"
    WHERE up."TP_RELATORIOS" = 'G'
        AND mve."TP_MVTO_ESTOQUE" IN ('S', 'P', 'D', 'C')
    GROUP BY
        itmve."CD_PRODUTO",
        up."VL_FATOR",
        up."TP_RELATORIOS",
        mve."TP_MVTO_ESTOQUE"
),
source_suprimentos AS (
    SELECT
        CONCAT(
            COALESCE(isol."CD_ITSOL_COM_KEY", '0'),
            COALESCE(io."CD_ITORD_PRO_KEY", '0'),
            COALESCE(ie."CD_ITENT_PRO", '0')
        )::NUMERIC AS "CD_SUPRIMENTO_KEY",
        h."CD_SOL_COM",
        h."CD_SETOR",
        h."CD_ESTOQUE",
        h."CD_MOT_PED",
        h."CD_MOT_CANCEL_SC",
        h."DT_SOL_COM",
        CASE
            WHEN (io."QT_COMPRADA" - io."QT_RECEBIDA") = 0 THEN 'Atendida'
            WHEN (io."QT_COMPRADA" - io."QT_RECEBIDA") = io."QT_CANCELADA" THEN 'Cancelada'
            ELSE h."TP_SITUACAO_SC"
        END AS "TP_SITUACAO_SC",
        h."SN_APROVADA",
        h."SN_URGENTE",
        h."SN_OPME",
        h."CD_ORD_COM",
        h."DT_ORD_COM",
        h."DT_AUTORIZACAO",
        h."CD_MOT_CANCEL_OC",
        CASE
            WHEN (io."QT_COMPRADA" - io."QT_RECEBIDA") = 0 THEN 'Atendida'
            WHEN (io."QT_COMPRADA" - io."QT_RECEBIDA") = io."QT_CANCELADA" THEN 'Cancelada'
            ELSE h."TP_SITUACAO_OC"
        END AS "TP_SITUACAO_OC",
        h."SN_AUTORIZADO_OC",
        ie."CD_FORNECEDOR",
        ie."NR_DOCUMENTO",
        ie."DT_ENTRADA",
        io."CD_UNI_PRO",
        h."CD_PRODUTO",
        isol."DT_CANCEL" AS "DT_CANCEL_SOL",
        COALESCE(isol."QT_SOLIC", 0) AS "QT_SOLIC_SOL",
        COALESCE(isol."QT_COMPRADA", 0) AS "QT_COMPRADA_SOL",
        COALESCE(isol."QT_ATENDIDA", 0) AS "QT_ATENDIDA_SOL",
        io."DT_CANCEL" AS "DT_CANCEL_ORD",
        COALESCE(io."QT_COMPRADA", 0) AS "QT_COMPRADA_ORD",
        COALESCE(io."QT_ATENDIDA", 0) AS "QT_ATENDIDA_ORD",
        COALESCE(io."QT_RECEBIDA", 0) AS "QT_RECEBIDA_ORD",
        COALESCE(io."QT_CANCELADA", 0) AS "QT_CANCELADA_ORD",
        COALESCE(io."VL_UNITARIO", 0) AS "VL_UNITARIO_ORD",
        ((COALESCE(io."QT_COMPRADA", 0) - COALESCE(ie."QT_ENTRADA", 0)) - COALESCE(io."QT_CANCELADA", 0)) AS "SALDO",
        COALESCE(ie."QT_ENTRADA", 0) AS "QT_ENTRADA_ENT",
        COALESCE(ie."QT_ATENDIDA", 0) AS "QT_ATENDIDA_ENT",
        COALESCE(ie."VL_UNITARIO", 0) AS "QT_UNITARIO_ENT",
        COALESCE(po."QT_ESTOQUE_ATUAL", 0) AS "QT_ESTOQUE_ATUAL",
        COALESCE(po."QT_CONSUMO_ATUAL", 0) AS "QT_CONSUMO_ATUAL",
        po."DT_ULTIMA_MOVIMENTACAO",
        COALESCE(tm."QT_MOVIMENTO", 0) AS "QT_MOVIMENTO"
    FROM source_sol_ord_com h
    LEFT JOIN source_itens_solicitacao isol ON h."CD_SOL_COM" = isol."CD_SOL_COM" AND h."CD_PRODUTO" = isol."CD_PRODUTO"
    LEFT JOIN source_itens_pedidos io ON h."CD_ORD_COM" = io."CD_ORD_COM" AND h."CD_PRODUTO" = io."CD_PRODUTO"
    LEFT JOIN source_itens_entradas ie ON h."CD_ORD_COM" = ie."CD_ORD_COM" AND h."CD_PRODUTO" = ie."CD_PRODUTO"
    LEFT JOIN source_est_pro po ON h."CD_PRODUTO" = po."CD_PRODUTO" AND h."CD_ESTOQUE" = po."CD_ESTOQUE"
    LEFT JOIN treats_qt_mov tm ON h."CD_PRODUTO" = tm."CD_PRODUTO"
    ORDER BY h."CD_PRODUTO"
),
treats AS (
    SELECT
        h."CD_SUPRIMENTO_KEY",
        h."CD_SOL_COM",
        h."CD_SETOR",
        h."CD_ESTOQUE",
        h."CD_MOT_PED",
        h."CD_MOT_CANCEL_SC",
        h."DT_SOL_COM",
        h."TP_SITUACAO_SC",
        h."SN_APROVADA",
        h."SN_URGENTE",
        h."SN_OPME",
        h."CD_ORD_COM",
        h."DT_ORD_COM",
        h."DT_AUTORIZACAO",
        h."CD_MOT_CANCEL_OC",
        h."TP_SITUACAO_OC",
        h."SN_AUTORIZADO_OC",
        h."CD_FORNECEDOR",
        h."NR_DOCUMENTO",
        h."DT_ENTRADA",
        h."CD_UNI_PRO",
        h."CD_PRODUTO",
        h."DT_CANCEL_SOL",
        h."QT_SOLIC_SOL",
        h."QT_COMPRADA_SOL",
        h."QT_ATENDIDA_SOL",
        h."DT_CANCEL_ORD",
        h."QT_COMPRADA_ORD",
        h."QT_ATENDIDA_ORD",
        h."QT_RECEBIDA_ORD",
        h."QT_CANCELADA_ORD",
        h."VL_UNITARIO_ORD",
        h."SALDO",
        h."QT_ENTRADA_ENT",
        h."QT_ATENDIDA_ENT",
        h."QT_UNITARIO_ENT",
        h."QT_ESTOQUE_ATUAL",
        h."QT_CONSUMO_ATUAL",
        h."DT_ULTIMA_MOVIMENTACAO",
        SUM(h."QT_MOVIMENTO") AS "QT_MOVIMENTO"
    FROM source_suprimentos h
    GROUP BY
        h."CD_SUPRIMENTO_KEY",
        h."CD_SOL_COM",
        h."CD_SETOR",
        h."CD_ESTOQUE",
        h."CD_MOT_PED",
        h."CD_MOT_CANCEL_SC",
        h."DT_SOL_COM",
        h."TP_SITUACAO_SC",
        h."SN_APROVADA",
        h."SN_URGENTE",
        h."SN_OPME",
        h."CD_ORD_COM",
        h."DT_ORD_COM",
        h."DT_AUTORIZACAO",
        h."CD_MOT_CANCEL_OC",
        h."TP_SITUACAO_OC",
        h."SN_AUTORIZADO_OC",
        h."CD_FORNECEDOR",
        h."NR_DOCUMENTO",
        h."DT_ENTRADA",
        h."CD_UNI_PRO",
        h."CD_PRODUTO",
        h."DT_CANCEL_SOL",
        h."QT_SOLIC_SOL",
        h."QT_COMPRADA_SOL",
        h."QT_ATENDIDA_SOL",
        h."DT_CANCEL_ORD",
        h."QT_COMPRADA_ORD",
        h."QT_ATENDIDA_ORD",
        h."QT_RECEBIDA_ORD",
        h."QT_CANCELADA_ORD",
        h."VL_UNITARIO_ORD",
        h."SALDO",
        h."QT_ENTRADA_ENT",
        h."QT_ATENDIDA_ENT",
        h."QT_UNITARIO_ENT",
        h."QT_ESTOQUE_ATUAL",
        h."QT_CONSUMO_ATUAL",
        h."DT_ULTIMA_MOVIMENTACAO"
)
SELECT * FROM treats