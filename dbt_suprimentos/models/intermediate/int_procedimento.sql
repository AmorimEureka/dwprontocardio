


WITH source_stg_procedimento_sus
    AS (
        SELECT
            "CD_PROCEDIMENTO" AS "CD_PROCEDIMENTO",
            "DS_PROCEDIMENTO"
        FROM {{ ref( 'stg_procedimento_sus' ) }}
),
source_stg_pro_fat
    AS (
        SELECT
            "CD_PRO_FAT" AS "CD_PROCEDIMENTO",
            "DS_PRO_FAT" AS "DS_PROCEDIMENTO"
        FROM {{ ref( 'stg_pro_fat' ) }}
),
source_stg_repasse_prestador
    AS (
        SELECT
            "CD_REPASSE"::VARCHAR(21) AS "CD_PROCEDIMENTO",
            "DS_REPASSE" AS "DS_PROCEDIMENTO"
        FROM {{ ref( 'stg_repasse_prestador' ) }}
)
treats
    AS (
        SELECT
            *
        FROM source_stg_procedimento_sus

        UNION ALL

        SELECT
            *
        FROM source_stg_pro_fat

        UNION ALL

        SELECT
            *
        FROM source_stg_repasse_prestador
    )
SELECT * FROM treats