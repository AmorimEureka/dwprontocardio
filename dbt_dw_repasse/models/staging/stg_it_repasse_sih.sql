{{
    config( materialized = 'incremental',
            unique_key = 'cd_it_repasse_sih',
        on_schema_change = 'sync_all_columns',
        tags = ['repasse']
    )
}}

WITH source_it_repasse_sih
    AS (
        SELECT
            cd_it_repasse_sih
            , cd_repasse
            , cd_reg_fat
            , cd_lancamento
            , cd_ati_med
            , cd_prestador_repasse
            , vl_repasse
        FROM {{ source('raw_repasse_mv', 'it_repasse_sih')}}
),
treats_key
    AS (
        SELECT
            sis.cd_it_repasse_sih
            , sis.cd_repasse
            , sis.cd_reg_fat
            , sis.cd_lancamento
            , sis.cd_ati_med
            , sis.cd_prestador_repasse
            , sis.vl_repasse
        FROM source_it_repasse_sih sis
        {% if is_incremental() %}
        WHERE sis.cd_it_repasse_sih::NUMERIC(10,0) > ( SELECT MAX(cd_it_repasse_sih) FROM {{this}} )
        {% endif %}
),
treats
    AS (
        SELECT
            cd_it_repasse_sih::NUMERIC(10,0)
            , cd_repasse::BIGINT
            , cd_reg_fat::BIGINT
            , cd_lancamento::BIGINT
            , cd_ati_med::BIGINT
            , cd_prestador_repasse::BIGINT
            , vl_repasse::NUMERIC(12,2)
        FROM treats_key
)
SELECT * FROM treats