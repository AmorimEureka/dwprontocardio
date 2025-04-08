{{
    config( materialized = 'incremental',
            unique_key = '"CD_ITSOL_COM_KEY"' )
}}

WITH source_itsol_com
    AS (
        SELECT
            NULLIF(sis."CD_SOL_COM", 'NaN') AS "CD_SOL_COM"
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
),
treats_key
    AS (
        SELECT
            CONCAT(sis."CD_SOL_COM", sis."CD_PRODUTO") AS "CD_ITSOL_COM_KEY"
            , sis."CD_SOL_COM"
            , sis."CD_PRODUTO"
            , sis."CD_UNI_PRO"
            , sis."CD_MOT_CANCEL"
            , sis."DT_CANCEL"
            , sis."QT_SOLIC"
            , sis."QT_COMPRADA"
            , sis."QT_ATENDIDA"
            , sis."SN_COMPRADO"
            , sis."DT_EXTRACAO"
        FROM source_itsol_com sis
        {% if is_incremental() %}
        WHERE CONCAT(sis."CD_SOL_COM", sis."CD_PRODUTO")::BIGINT > ( SELECT MAX("CD_ITSOL_COM_KEY") FROM {{this}} )
        {% endif %}
),
treats
    AS (
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
        FROM treats_key
    )
SELECT * FROM treats