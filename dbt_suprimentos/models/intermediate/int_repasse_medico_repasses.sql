

WITH source_repasse
    AS (
        SELECT
            "CD_REPASSE",
            "DT_COMPETENCIA",
            "DT_REPASSE",
            "TP_REPASSE"
        FROM {{ ref('stg_repasse')}}
),
source_item_repasse
    AS (
        SELECT
            "CD_REPASSE",
            "CD_REG_AMB",
            "CD_LANCAMENTO_AMB",
            "CD_REG_FAT",
            "CD_LANCAMENTO_FAT",
            "CD_ATI_MED",
            "CD_PRESTADOR_REPASSE",
            "VL_REPASSE"
        FROM {{ ref('stg_it_repasse')}}
),
source_item_repasse_sih
    AS (
        SELECT
           "CD_REPASSE",
            NULL::NUMERIC(10,0) AS "CD_REG_AMB",
            NULL::NUMERIC(10,0) AS "CD_LANCAMENTO_AMB",
            "CD_REG_FAT",
            "CD_LANCAMENTO" AS "CD_LANCAMENTO_FAT",
            "CD_ATI_MED",
            "CD_PRESTADOR_REPASSE",
            "VL_REPASSE"
        FROM {{ ref('stg_it_repasse_sih')}}
),
treats_repasse
    AS (
        SELECT
            ir."CD_REPASSE",
            ir."CD_REG_AMB",
            ir."CD_LANCAMENTO_AMB",
            CASE
                WHEN ir."CD_REG_AMB" IS NULL AND ir."CD_LANCAMENTO_AMB" IS NULL THEN
                    NULL
                ELSE CONCAT(
                    COALESCE(ir."CD_REG_AMB", 0)::NUMERIC(10,0),
                    COALESCE(ir."CD_LANCAMENTO_AMB", 0)::NUMERIC(10,0)
                    )::NUMERIC(20,0)
            END AS "CD_ITREG_AMB_KEY",
            ir."CD_REG_FAT",
            ir."CD_LANCAMENTO_FAT",
            CASE
                WHEN ir."CD_REG_FAT" IS NULL AND ir."CD_LANCAMENTO_FAT" IS NULL THEN
                    NULL
                ELSE CONCAT(
                    COALESCE(ir."CD_REG_FAT", 0)::NUMERIC(10,0),
                    COALESCE(ir."CD_LANCAMENTO_FAT", 0)::NUMERIC(10,0)
                    )::NUMERIC(20,0)
            END AS "CD_ITREG_FAT_KEY",
            ir."CD_ATI_MED",
            ir."CD_PRESTADOR_REPASSE",
            ir."VL_REPASSE",
            r."DT_COMPETENCIA",
            r."DT_REPASSE",
            r."TP_REPASSE"
        FROM source_item_repasse ir
        LEFT JOIN source_repasse r ON ir."CD_REPASSE" = r."CD_REPASSE"
),
treats_repasse_sih
    AS (
        SELECT
            sih."CD_REPASSE",
            sih."CD_REG_AMB",
            sih."CD_LANCAMENTO_AMB",
            NULL::NUMERIC(20,0) AS "CD_ITREG_AMB_KEY",
            sih."CD_REG_FAT",
            sih."CD_LANCAMENTO_FAT",
            CASE
                WHEN sih."CD_REG_FAT" IS NULL AND sih."CD_LANCAMENTO_FAT" IS NULL THEN
                    NULL
                ELSE CONCAT(
                    COALESCE(sih."CD_REG_FAT", 0)::NUMERIC(10,0),
                    COALESCE(sih."CD_LANCAMENTO_FAT", 0)::NUMERIC(10,0)
                    )::NUMERIC(20,0)
            END AS "CD_ITREG_FAT_KEY",
            sih."CD_ATI_MED",
            sih."CD_PRESTADOR_REPASSE",
            sih."VL_REPASSE",
            rish."DT_COMPETENCIA",
            rish."DT_REPASSE",
            rish."TP_REPASSE"
        FROM source_item_repasse_sih sih
        LEFT JOIN source_repasse rish ON sih."CD_REPASSE" = rish."CD_REPASSE"
),
treats
    AS (
        SELECT
            *
        FROM treats_repasse

        UNION ALL

        SELECT
            *
        FROM treats_repasse_sih
)
SELECT * FROM treats