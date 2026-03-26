{{
    config(
        tags = ['repasse']
    )
}}

WITH source_repasse
    AS (
        SELECT
            cd_repasse,
            dt_competencia,
            dt_repasse,
            tp_repasse
        FROM {{ ref('stg_repasse')}}
),
source_item_repasse
    AS (
        SELECT
            cd_repasse,
            cd_reg_amb,
            cd_lancamento_amb,
            cd_reg_fat,
            cd_lancamento_fat,
            cd_ati_med,
            cd_prestador_repasse,
            vl_repasse
        FROM {{ ref('stg_it_repasse')}}
),
source_item_repasse_sih
    AS (
        SELECT
           cd_repasse,
            NULL::NUMERIC(10,0) AS cd_reg_amb,
            NULL::NUMERIC(10,0) AS cd_lancamento_amb,
            cd_reg_fat,
            cd_lancamento AS cd_lancamento_fat,
            cd_ati_med,
            cd_prestador_repasse,
            vl_repasse
        FROM {{ ref('stg_it_repasse_sih')}}
),
treats_repasse
    AS (
        SELECT
            ir.cd_repasse,
            ir.cd_reg_amb,
            ir.cd_lancamento_amb,
            CASE
                WHEN ir.cd_reg_amb IS NULL AND ir.cd_lancamento_amb IS NULL THEN
                    NULL
                ELSE CONCAT(
                    COALESCE(ir.cd_reg_amb, 0)::NUMERIC(10,0),
                    COALESCE(ir.cd_lancamento_amb, 0)::NUMERIC(10,0)
                    )::NUMERIC(20,0)
            END AS cd_itreg_amb_key,
            ir.cd_reg_fat,
            ir.cd_lancamento_fat,
            CASE
                WHEN ir.cd_reg_fat IS NULL AND ir.cd_lancamento_fat IS NULL THEN
                    NULL
                ELSE CONCAT(
                    COALESCE(ir.cd_reg_fat, 0)::NUMERIC(10,0),
                    COALESCE(ir.cd_lancamento_fat, 0)::NUMERIC(10,0)
                    )::NUMERIC(20,0)
            END AS cd_itreg_fat_key,
            ir.cd_ati_med,
            ir.cd_prestador_repasse,
            ir.vl_repasse,
            r.dt_competencia,
            r.dt_repasse,
            r.tp_repasse
        FROM source_item_repasse ir
        LEFT JOIN source_repasse r ON ir.cd_repasse = r.cd_repasse
),
treats_repasse_sih
    AS (
        SELECT
            sih.cd_repasse,
            sih.cd_reg_amb,
            sih.cd_lancamento_amb,
            NULL::NUMERIC(20,0) AS cd_itreg_amb_key,
            sih.cd_reg_fat,
            sih.cd_lancamento_fat,
            CASE
                WHEN sih.cd_reg_fat IS NULL AND sih.cd_lancamento_fat IS NULL THEN
                    NULL
                ELSE CONCAT(
                    COALESCE(sih.cd_reg_fat, 0)::NUMERIC(10,0),
                    COALESCE(sih.cd_lancamento_fat, 0)::NUMERIC(10,0)
                    )::NUMERIC(20,0)
            END AS cd_itreg_fat_key,
            sih.cd_ati_med,
            sih.cd_prestador_repasse,
            sih.vl_repasse,
            rish.dt_competencia,
            rish.dt_repasse,
            rish.tp_repasse
        FROM source_item_repasse_sih sih
        LEFT JOIN source_repasse rish ON sih.cd_repasse = rish.cd_repasse
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