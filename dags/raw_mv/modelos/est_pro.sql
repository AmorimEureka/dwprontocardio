

WITH dados_base
    AS (
        SELECT
            ep.CD_ESTOQUE,
            ep.CD_PRODUTO,
            ep.CD_LOCALIZACAO,
            ep.TP_CLASSIFICACAO_ABC,
            ep.DT_ULTIMA_MOVIMENTACAO,
            ep.QT_ESTOQUE_ATUAL / verif_vl_fator_prod(ep.CD_PRODUTO, 'G') AS QT_ESTOQUE_ATUAL,
            ep.QT_CONSUMO_ATUAL / verif_vl_fator_prod(ep.CD_PRODUTO, 'G') AS QT_CONSUMO_ATUAL
            -- (
            --     SELECT
            --         SUM(
            --             itmvto_estoque.qt_movimentacao * uni_pro.vl_fator *
            --             DECODE(mvto_estoque.tp_mvto_estoque, 'D', -1, 'C', -1, 1)
            --         )
            --     FROM dbamv.mvto_estoque,
            --         dbamv.itmvto_estoque,
            --         dbamv.uni_pro
            --     WHERE mvto_estoque.cd_mvto_estoque = itmvto_estoque.cd_mvto_estoque
            --         AND itmvto_estoque.cd_produto = ep.cd_produto
            --         AND itmvto_estoque.cd_uni_pro = uni_pro.cd_uni_pro
            --         AND mvto_estoque.tp_mvto_estoque IN ('S', 'P', 'D', 'C')
            --         AND mvto_estoque.dt_mvto_estoque
            --             BETWEEN TRUNC(SYSDATE) - 90 AND TRUNC(SYSDATE) + 0.99999
            -- ) / verif_vl_fator_prod(ep.CD_PRODUTO, 'G') AS QT_MVTO
        FROM DBAMV.EST_PRO ep
)
SELECT
    CD_ESTOQUE,
    CD_PRODUTO,
    CD_LOCALIZACAO,
    TP_CLASSIFICACAO_ABC,
    DT_ULTIMA_MOVIMENTACAO,
    QT_ESTOQUE_ATUAL,
    QT_CONSUMO_ATUAL
    -- ROUND(NVL(QT_MVTO, 0), 0) AS QT_MVTO,
    -- ROUND(NVL(QT_MVTO / 3, 0), 0) AS QT_MENSAL,
    -- ROUND(NVL(QT_MVTO / 90, 0), 0) AS QT_DIARIO,
    -- CASE
    --     WHEN QT_MVTO <= 0 THEN 99
    --     ELSE ROUND(QT_ESTOQUE_ATUAL / (QT_MVTO / 90), 0)
    -- END AS QTD_DIAS_ESTOQUE
FROM dados_base

