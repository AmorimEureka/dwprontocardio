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
            ir."CD_REG_FAT",
            ir."CD_LANCAMENTO_FAT",
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
            sih."CD_REG_FAT",
            sih."CD_LANCAMENTO_FAT",
            sih."CD_ATI_MED",
            sih."CD_PRESTADOR_REPASSE",
            sih."VL_REPASSE",
            rish."DT_COMPETENCIA",
            rish."DT_REPASSE",
            rish."TP_REPASSE"
        FROM source_item_repasse_sih sih
        LEFT JOIN source_repasse rish ON sih."CD_REPASSE" = rish."CD_REPASSE"
),
treats_repasse_consolidado
    AS (
        SELECT
            *
        FROM treats_repasse

        UNION ALL

        SELECT
            *
        FROM treats_repasse_sih
),
source_repasse_prestador
    AS (
        SELECT
            "CD_REPASSE",
            "CD_PRESTADOR_REPASSE",
            "CD_REPASSE" AS "CD_ITREG_KEY",
            "DT_COMPETENCIA",
            "DT_REPASSE",
            "TP_REPASSE",
            "VL_REPASSE"
        FROM {{ ref('stg_repasse_prestador')}}
),
source_atendimento
    AS (
        SELECT
            "CD_ATENDIMENTO",
            "CD_PACIENTE",
            "CD_PRESTADOR"
        FROM {{ ref('stg_atendime')}}
),
source_pro_fat
    AS (
        SELECT
            "CD_PRO_FAT",
            "CD_GRU_PRO",
            "DS_PRO_FAT"
        FROM {{ ref('stg_pro_fat')}}
),
source_regra_ambulatorio
    AS (
        SELECT
            "CD_REG_AMB",
            "CD_REMESSA",
            "DT_REMESSA"
        FROM {{ ref('stg_reg_amb')}}
),
source_item_regra_ambulatorio
    AS (
        SELECT
            "CD_ITREG_AMB_KEY",
            "CD_PRO_FAT",
            "CD_REG_AMB",
            "CD_PRESTADOR",
            "CD_ATI_MED",
            "CD_LANCAMENTO",
            "CD_GRU_FAT",
            "CD_CONVENIO",
            "CD_ATENDIMENTO",
            "DT_PRODUCAO",
            "DT_FECHAMENTO",
            "DT_ITREG_AMB",
            "SN_FECHADA",
            "SN_REPASSADO",
            "SN_PERTENCE_PACOTE",
            "VL_UNITARIO",
            "VL_TOTAL_CONTA",
            "VL_BASE_REPASSADO"
        FROM {{ ref('stg_itreg_amb')}}
),
treats_regra_ambulatorio
    AS (
        SELECT
            ira."CD_ITREG_AMB_KEY",
            pf."CD_PRO_FAT",
            ira."CD_REG_AMB",
            ira."CD_PRESTADOR",
            sa."CD_PACIENTE",
            ira."CD_ATI_MED",
            ira."CD_LANCAMENTO",
            pf."CD_GRU_PRO",
            ira."CD_GRU_FAT",
            ira."CD_CONVENIO",
            ira."CD_ATENDIMENTO",
            ra."CD_REMESSA",
            pf."DS_PRO_FAT",
            ra."DT_REMESSA",
            ira."DT_PRODUCAO",
            ira."DT_FECHAMENTO",
            ira."DT_ITREG_AMB",
            ira."SN_FECHADA",
            ira."SN_REPASSADO",
            ira."SN_PERTENCE_PACOTE",
            ira."VL_UNITARIO",
            ira."VL_TOTAL_CONTA",
            ira."VL_BASE_REPASSADO"
        FROM source_item_regra_ambulatorio ira
        LEFT JOIN source_pro_fat pf ON ira."CD_PRO_FAT" = pf."CD_PRO_FAT"
        LEFT JOIN source_regra_ambulatorio ra ON ira."CD_REG_AMB" = ra."CD_REG_AMB"
        LEFT JOIN source_atendimento sa ON ira."CD_ATENDIMENTO" = sa."CD_ATENDIMENTO"
),
source_regra_faturamento
    AS (
        SELECT
            "CD_REG_FAT",
            "CD_CONVENIO",
            "CD_ATENDIMENTO",
            "CD_REMESSA",
            "DT_REMESSA"
        FROM {{ ref('stg_reg_fat')}}
),
source_item_regra_faturamento
    AS (
        SELECT
            "CD_ITREG_FAT_KEY",
            "CD_PRO_FAT",
            "CD_REG_FAT",
            "CD_PRESTADOR",
            "CD_ATI_MED",
            "CD_LANCAMENTO",
            "CD_GRU_FAT",
            "CD_PROCEDIMENTO",
            "DT_PRODUCAO",
            "DT_ITREG_FAT",
            "SN_REPASSADO",
            "SN_PERTENCE_PACOTE",
            "VL_UNITARIO",
            "VL_TOTAL_CONTA",
            "VL_BASE_REPASSADO"
        FROM {{ ref('stg_itreg_fat')}}
),
treats_regra_faturamento
    AS (
        SELECT
            irf."CD_ITREG_FAT_KEY",
            CASE WHEN pf."CD_PRO_FAT" = 'X0000000' THEN
                irf."CD_PROCEDIMENTO"
            ELSE pf."CD_PRO_FAT"
            END AS "CD_PRO_FAT",
            irf."CD_REG_FAT",
            irf."CD_PRESTADOR",
            sa."CD_PACIENTE",
            irf."CD_ATI_MED",
            irf."CD_LANCAMENTO",
            pf."CD_GRU_PRO",
            irf."CD_GRU_FAT",
            rf."CD_CONVENIO",
            rf."CD_ATENDIMENTO",
            rf."CD_REMESSA",
            pf."DS_PRO_FAT",
            rf."DT_REMESSA",
            irf."DT_PRODUCAO",
            NULL::DATE AS "DT_FECHAMENTO",
            irf."DT_ITREG_FAT",
            NULL::VARCHAR AS "SN_FECHADA",
            irf."SN_REPASSADO",
            irf."SN_PERTENCE_PACOTE",
            irf."VL_UNITARIO",
            irf."VL_TOTAL_CONTA",
            irf."VL_BASE_REPASSADO"
        FROM source_item_regra_faturamento irf
        LEFT JOIN source_pro_fat pf ON irf."CD_PRO_FAT" = pf."CD_PRO_FAT"
        LEFT JOIN source_regra_faturamento rf ON irf."CD_REG_FAT" = rf."CD_REG_FAT"
        LEFT JOIN source_atendimento sa ON rf."CD_ATENDIMENTO" = sa."CD_ATENDIMENTO"
),
treats_regra_ambulatorio_sem_remessa
    AS (
        SELECT
            NULL AS "CD_REPASSE",
            tra."CD_REG_AMB" AS "CD_REGRA",
            tra."CD_LANCAMENTO",
            tra."CD_ITREG_AMB_KEY" AS "CD_ITREG_KEY",
            tra."CD_PRO_FAT",
            tra."CD_GRU_PRO",
            tra."CD_GRU_FAT",
            sa."CD_ATENDIMENTO",
            tra."CD_REMESSA",
            tra."CD_CONVENIO",
            NULL AS "CD_ATI_MED",
            sa."CD_PRESTADOR" AS "CD_PRESTADOR_REPASSE",
            sa."CD_PACIENTE" AS "CD_PACIENTE",
            tra."DT_ITREG_AMB" AS "DT_ITREGRA",
            NULL AS "DT_COMPETENCIA",
            NULL AS "DT_REPASSE",
            tra."DT_PRODUCAO",
            tra."DT_FECHAMENTO",
            NULL AS "TP_REPASSE",
            'AMBULATORIO'::VARCHAR(12) AS "TP_REGRA",
            tra."SN_FECHADA",
            tra."SN_REPASSADO",
            tra."SN_PERTENCE_PACOTE",
            NULL AS "VL_REPASSE",
            tra."VL_UNITARIO",
            tra."VL_TOTAL_CONTA",
            tra."VL_BASE_REPASSADO"
        FROM source_atendimento sa
        INNER JOIN treats_regra_ambulatorio tra ON sa."CD_ATENDIMENTO" = tra."CD_ATENDIMENTO"
),
treats_regra_faturamento_sem_remessa
    AS (
        SELECT
            NULL AS "CD_REPASSE",
            trf."CD_REG_FAT" AS "CD_REGRA",
            trf."CD_LANCAMENTO",
            trf."CD_ITREG_FAT_KEY"  AS "CD_ITREG_KEY",
            trf."CD_PRO_FAT",
            trf."CD_GRU_PRO",
            trf."CD_GRU_FAT",
            sa."CD_ATENDIMENTO",
            trf."CD_REMESSA",
            trf."CD_CONVENIO",
            NULL AS "CD_ATI_MED",
            sa."CD_PRESTADOR" AS "CD_PRESTADOR_REPASSE",
            sa."CD_PACIENTE" AS "CD_PACIENTE",
            trf."DT_ITREG_FAT" AS "DT_ITREGRA",
            NULL AS "DT_COMPETENCIA",
            NULL AS "DT_REPASSE",
            trf."DT_PRODUCAO",
            trf."DT_FECHAMENTO",
            NULL AS "TP_REPASSE",
            'HOSPITALAR'::VARCHAR(12) AS "TP_REGRA",
            trf."SN_FECHADA",
            trf."SN_REPASSADO",
            trf."SN_PERTENCE_PACOTE",
            NULL AS "VL_REPASSE",
            trf."VL_UNITARIO",
            trf."VL_TOTAL_CONTA",
            trf."VL_BASE_REPASSADO"
        FROM source_atendimento sa
        INNER JOIN treats_regra_faturamento trf ON sa."CD_ATENDIMENTO" = trf."CD_ATENDIMENTO"
),
treats_regra_sem_remessa_consolidado
    AS (
        SELECT * FROM treats_regra_ambulatorio_sem_remessa
        UNION ALL
        SELECT * FROM treats_regra_faturamento_sem_remessa
),
treats_repasse_regra_ambulatorio
    AS (
        SELECT
            trc."CD_REPASSE",
            tra."CD_REG_AMB" AS "CD_REGRA",
            tra."CD_LANCAMENTO",
            tra."CD_ITREG_AMB_KEY" AS "CD_ITREG_KEY",
            tra."CD_PRO_FAT",
            tra."CD_GRU_PRO",
            tra."CD_GRU_FAT",
            tra."CD_ATENDIMENTO",
            tra."CD_REMESSA",
            tra."CD_CONVENIO",
            trc."CD_ATI_MED",
            trc."CD_PRESTADOR_REPASSE",
            tra."CD_PACIENTE" AS "CD_PACIENTE",
            tra."DT_ITREG_AMB" AS "DT_ITREGRA",
            trc."DT_COMPETENCIA",
            trc."DT_REPASSE",
            tra."DT_PRODUCAO",
            tra."DT_FECHAMENTO",
            trc."TP_REPASSE",
            'AMBULATORIO'::VARCHAR(12) AS "TP_REGRA",
            tra."SN_FECHADA",
            tra."SN_REPASSADO",
            tra."SN_PERTENCE_PACOTE",
            trc."VL_REPASSE",
            tra."VL_UNITARIO",
            tra."VL_TOTAL_CONTA",
            tra."VL_BASE_REPASSADO"
        FROM treats_repasse_consolidado trc
        INNER JOIN treats_regra_ambulatorio tra  ON trc."CD_REG_AMB" = tra."CD_REG_AMB" AND trc."CD_LANCAMENTO_AMB" = tra."CD_LANCAMENTO"
),
treats_repasse_regra_faturamento
    AS (
        SELECT
            trc."CD_REPASSE",
            trf."CD_REG_FAT" AS "CD_REGRA",
            trf."CD_LANCAMENTO",
            trf."CD_ITREG_FAT_KEY"  AS "CD_ITREG_KEY",
            trf."CD_PRO_FAT",
            trf."CD_GRU_PRO",
            trf."CD_GRU_FAT",
            trf."CD_ATENDIMENTO",
            trf."CD_REMESSA",
            trf."CD_CONVENIO",
            trc."CD_ATI_MED",
            trc."CD_PRESTADOR_REPASSE",
            trf."CD_PACIENTE" AS "CD_PACIENTE",
            trf."DT_ITREG_FAT" AS "DT_ITREGRA",
            trc."DT_COMPETENCIA",
            trc."DT_REPASSE",
            trf."DT_PRODUCAO",
            trf."DT_FECHAMENTO",
            trc."TP_REPASSE",
            'HOSPITALAR'::VARCHAR(12) AS "TP_REGRA",
            trf."SN_FECHADA",
            trf."SN_REPASSADO",
            trf."SN_PERTENCE_PACOTE",
            trc."VL_REPASSE",
            trf."VL_UNITARIO",
            trf."VL_TOTAL_CONTA",
            trf."VL_BASE_REPASSADO"
        FROM treats_repasse_consolidado trc
        INNER JOIN treats_regra_faturamento trf  ON trc."CD_REG_FAT" = trf."CD_REG_FAT" AND trc."CD_LANCAMENTO_FAT" = trf."CD_LANCAMENTO"
),
treats_repasse_manual
    AS (
        SELECT
            rp."CD_REPASSE",
            NULL AS "CD_REGRA",
            NULL AS "CD_LANCAMENTO",
            rp."CD_ITREG_KEY",
            NULL AS "CD_PRO_FAT",
            NULL AS "CD_GRU_PRO",
            NULL AS "CD_GRU_FAT",
            NULL AS "CD_ATENDIMENTO",
            0 AS "CD_REMESSA",
            NULL AS "CD_CONVENIO",
            rp."CD_REPASSE" AS "CD_ATI_MED",
            rp."CD_PRESTADOR_REPASSE",
            NULL AS "CD_PACIENTE",
            NULL AS "DT_ITREGRA",
            rp."DT_COMPETENCIA",
            rp."DT_REPASSE",
            NULL AS "DT_PRODUCAO",
            NULL AS "DT_FECHAMENTO",
            rp."TP_REPASSE",
            'LANC. MANUAL'::VARCHAR(12) AS "TP_REGRA",
            NULL AS "SN_FECHADA",
            NULL AS "SN_REPASSADO",
            NULL AS "SN_PERTENCE_PACOTE",
            rp."VL_REPASSE",
            NULL AS "VL_UNITARIO",
            NULL AS "VL_TOTAL_CONTA",
            NULL AS "VL_BASE_REPASSADO"
        FROM source_repasse_prestador rp
),
treats
    AS (
        SELECT
            *
        FROM treats_repasse_regra_ambulatorio

        UNION ALL

        SELECT
            *
        FROM treats_repasse_regra_faturamento

        UNION ALL

        SELECT
            "CD_REPASSE"::BIGINT,
            "CD_REGRA"::NUMERIC(10,0),
            "CD_LANCAMENTO"::NUMERIC(10,0),
            "CD_ITREG_KEY"::NUMERIC(20,0),
            "CD_PRO_FAT"::VARCHAR(21),
            "CD_GRU_PRO"::BIGINT,
            "CD_GRU_FAT"::BIGINT,
            "CD_ATENDIMENTO"::BIGINT,
            "CD_REMESSA"::BIGINT,
            "CD_CONVENIO"::BIGINT,
            "CD_ATI_MED"::BIGINT,
            "CD_PRESTADOR_REPASSE"::BIGINT,
            "CD_PACIENTE"::BIGINT,
            "DT_ITREGRA"::TIMESTAMP,
            "DT_COMPETENCIA"::TIMESTAMP,
            "DT_REPASSE"::TIMESTAMP,
            "DT_PRODUCAO"::TIMESTAMP,
            "DT_FECHAMENTO"::TIMESTAMP,
            "TP_REPASSE"::VARCHAR(1),
            "TP_REGRA"::VARCHAR(12),
            "SN_FECHADA"::VARCHAR(1),
            "SN_REPASSADO"::VARCHAR(1),
            "SN_PERTENCE_PACOTE"::VARCHAR(1),
            "VL_REPASSE"::NUMERIC(12, 2),
            "VL_UNITARIO"::NUMERIC(14,4),
            "VL_TOTAL_CONTA"::NUMERIC(12,2),
            "VL_BASE_REPASSADO"::NUMERIC(12,2)
        FROM treats_repasse_manual

        UNION ALL

        SELECT
            "CD_REPASSE"::BIGINT,
            "CD_REGRA"::NUMERIC(10,0),
            "CD_LANCAMENTO"::NUMERIC(10,0),
            "CD_ITREG_KEY"::NUMERIC(20,0),
            "CD_PRO_FAT"::VARCHAR(21),
            "CD_GRU_PRO"::BIGINT,
            "CD_GRU_FAT"::BIGINT,
            "CD_ATENDIMENTO"::BIGINT,
            "CD_REMESSA"::BIGINT,
            "CD_CONVENIO"::BIGINT,
            "CD_ATI_MED"::BIGINT,
            "CD_PRESTADOR_REPASSE"::BIGINT,
            "CD_PACIENTE"::BIGINT,
            "DT_ITREGRA"::TIMESTAMP,
            "DT_COMPETENCIA"::TIMESTAMP,
            "DT_REPASSE"::TIMESTAMP,
            "DT_PRODUCAO"::TIMESTAMP,
            "DT_FECHAMENTO"::TIMESTAMP,
            "TP_REPASSE"::VARCHAR(1),
            "TP_REGRA"::VARCHAR(12),
            "SN_FECHADA"::VARCHAR(1),
            "SN_REPASSADO"::VARCHAR(1),
            "SN_PERTENCE_PACOTE"::VARCHAR(1),
            "VL_REPASSE"::NUMERIC(12, 2),
            "VL_UNITARIO"::NUMERIC(14,4),
            "VL_TOTAL_CONTA"::NUMERIC(12,2),
            "VL_BASE_REPASSADO"::NUMERIC(12,2)
        FROM treats_regra_sem_remessa_consolidado
)
SELECT * FROM treats


