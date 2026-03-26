{{
    config( materialized = 'incremental',
            unique_key = 'cd_it_repasse',
        on_schema_change = 'sync_all_columns',
        tags = ['repasse']
    )
}}


WITH source_it_repasse
    AS(
        SELECT
            sis.cd_it_repasse,
            sis.cd_repasse,
            sis.cd_reg_amb,
            sis.cd_lancamento_amb,
            sis.cd_reg_fat,
            sis.cd_lancamento_fat,
            sis.cd_ati_med,
            sis.cd_prestador_repasse,
            sis.vl_repasse
        FROM {{ source('raw_repasse_mv', 'it_repasse') }} sis
        {% if is_incremental() %}
        WHERE sis.cd_it_repasse::NUMERIC(10,0) > ( SELECT MAX(cd_it_repasse) FROM {{this}} )
        {% endif %}
),
treats
    AS(
        SELECT
            cd_it_repasse::NUMERIC(10,0),
            cd_repasse::BIGINT,
            cd_reg_amb::BIGINT,
            cd_lancamento_amb::BIGINT,
            cd_reg_fat::BIGINT,
            cd_lancamento_fat::BIGINT,
            cd_ati_med::BIGINT,
            cd_prestador_repasse::BIGINT,
            vl_repasse::NUMERIC(12,2)
        FROM source_it_repasse
)
SELECT * FROM treats