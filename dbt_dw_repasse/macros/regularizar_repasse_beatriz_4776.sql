{% macro regularizar_repasse_beatriz_4776() %}
    {#
      Correção pontual e auditável para a substituição do repasse 4775 pelo 4776
      da prestadora 1187. A origem já não contém o 4775, mas cargas incrementais
      anteriores o preservaram no raw, staging e mart.

      O repasse 4776 também é removido somente da fato para ser reconstruído pelo
      modelo com uma linha por sk_repasse_medico após a correção do join incremental.
    #}
    {% set statements = [
        "DELETE FROM mart_repasse.f_repasses_medicos WHERE cd_prestador_repasse = 1187 AND cd_repasse IN (4775, 4776)",
        "DELETE FROM staging_repasse.stg_it_repasse WHERE cd_prestador_repasse = 1187 AND cd_repasse = 4775",
        "DELETE FROM raw_repasse_mv.it_repasse WHERE cd_prestador_repasse::BIGINT = 1187 AND cd_repasse::BIGINT = 4775",
        "DELETE FROM staging_repasse.stg_repasse_prestador WHERE cd_prestador_repasse = 1187 AND cd_repasse = 4775",
        "DELETE FROM raw_repasse_mv.repasse_prestador WHERE cd_prestador_repasse::BIGINT = 1187 AND cd_repasse::BIGINT = 4775",
        "DELETE FROM staging_repasse.stg_repasse WHERE cd_repasse = 4775",
        "DELETE FROM raw_repasse_mv.repasse WHERE cd_repasse::BIGINT = 4775"
    ] %}

    {% for statement in statements %}
        {% do run_query(statement) %}
    {% endfor %}

    {% do log("Repasse obsoleto 4775 removido e fato 4776 preparada para reconstrução.", info=true) %}
{% endmacro %}
