{{
    config( materialized = 'incremental',
            unique_key = 'CD_ITSOL_COM_KEY',
            incremental_strategy = 'append' )
}}

WITH source_itsol_com
    AS (
        SELECT
            CONCAT(NULLIF(sis."CD_SOL_COM", 'NaN'), NULLIF(sis."CD_PRODUTO", 'NaN')) AS "CD_ITSOL_COM_KEY"
            , NULLIF(sis."CD_SOL_COM", 'NaN') AS "CD_SOL_COM"
            , NULLIF(sis."CD_PRODUTO", 'NaN') AS "CD_PRODUTO"
            , NULLIF(sis."CD_UNI_PRO", 'NaN') AS "CD_UNI_PRO"
            , NULLIF(SPLIT_PART(sis."CD_MOT_CANCEL"::TEXT,'.', 1), 'NaN') AS "CD_MOT_CANCEL"
            , sis."DT_CANCEL"
            , NULLIF(sis."QT_SOLIC", 'NaN') AS "QT_SOLIC"
            , NULLIF(sis."QT_COMPRADA", 'NaN') AS "QT_COMPRADA"
            , NULLIF(sis."QT_ATENDIDA", 'NaN') AS "QT_ATENDIDA"
            , NULLIF(sis."SN_COMPRADO", 'NaN') AS "SN_COMPRADO"
            , sis."DT_EXTRACAO"
        FROM {{ source('raw_mv' , 'itsol_com') }} sis
        {% if is_incremental() %}
        WHERE sis."DT_EXTRACAO" > (SELECT COALESCE(MAX("DT_EXTRACAO"), '2024-01-01'::timestamp) FROM {{ this }})
        {% endif %}
),
treats
    AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY "CD_ITSOL_COM_KEY" ORDER BY "DT_EXTRACAO" DESC) as rn
        FROM (
            SELECT
                "CD_ITSOL_COM_KEY"::BIGINT
                , "CD_SOL_COM"::BIGINT
                , "CD_PRODUTO"::BIGINT
                , "CD_UNI_PRO"::BIGINT
                , "CD_MOT_CANCEL"::BIGINT
                , "DT_CANCEL"::TIMESTAMP
                , "QT_SOLIC"::NUMERIC(11,3)
                , "QT_COMPRADA"::NUMERIC(11,3)
                , "QT_ATENDIDA"::NUMERIC(11,3)
                , "SN_COMPRADO"::VARCHAR(1)
                , "DT_EXTRACAO"::TIMESTAMP
            FROM source_itsol_com
        ) base
    )
SELECT
    "CD_ITSOL_COM_KEY",
    "CD_SOL_COM",
    "CD_PRODUTO",
    "CD_UNI_PRO",
    "CD_MOT_CANCEL",
    "DT_CANCEL",
    "QT_SOLIC",
    "QT_COMPRADA",
    "QT_ATENDIDA",
    "SN_COMPRADO",
    "DT_EXTRACAO"
FROM treats WHERE rn = 1