{{
    config(
        tags = ['repasse']
    )
}}


WITH source_stg_procedimento_sus
    AS (
        SELECT
            cd_procedimento::VARCHAR(21) AS cd_procedimento,
            ds_procedimento::VARCHAR(250) AS ds_procedimento
        FROM {{ ref( 'stg_procedimento_sus' ) }}
),
source_stg_pro_fat
    AS (
        SELECT
            cd_pro_fat::VARCHAR(21) AS cd_procedimento,
            ds_pro_fat::VARCHAR(250) AS ds_procedimento
        FROM {{ ref( 'stg_pro_fat' ) }}
),
source_stg_repasse_prestador
    AS (
        SELECT
            cd_repasse::VARCHAR(21) AS cd_procedimento,
            ds_repasse::VARCHAR(250) AS ds_procedimento
        FROM {{ ref( 'stg_repasse_prestador' ) }}
),
treats
    AS (
        SELECT
            cd_procedimento,
            ds_procedimento
        FROM source_stg_procedimento_sus

        UNION ALL

        SELECT
            cd_procedimento,
            ds_procedimento
        FROM source_stg_pro_fat

        UNION ALL

        SELECT
            cd_procedimento,
            ds_procedimento
        FROM source_stg_repasse_prestador
    )
SELECT * FROM treats