

WITH itmvto_estoquee AS (
	SELECT 
	    itmv.CD_ITMVTO_ESTOQUE,
	    itmv.CD_MVTO_ESTOQUE,
	    itmv.CD_PRODUTO,
	    itmv.CD_UNI_PRO,
	    itmv.CD_LOTE,
	    itmv.CD_ITENT_PRO,
	    itmv.CD_FORNECEDOR,
	    itmv.CD_ITPRE_MED,
	    itmv.DT_VALIDADE,
	    CASE 
	        WHEN EXTRACT(YEAR FROM itmv.DH_MVTO_ESTOQUE) < 0 THEN 
	            TO_TIMESTAMP(
	                ABS(EXTRACT(YEAR FROM itmv.DH_MVTO_ESTOQUE)) || '-' || 
	                LPAD(EXTRACT(MONTH FROM itmv.DH_MVTO_ESTOQUE), 2, '0') || '-' || 
	                LPAD(EXTRACT(DAY FROM itmv.DH_MVTO_ESTOQUE), 2, '0') || ' ' ||
	                LPAD(TO_CHAR(itmv.DH_MVTO_ESTOQUE, 'HH24'), 2, '0') || ':' ||
	                LPAD(TO_CHAR(itmv.DH_MVTO_ESTOQUE, 'MI'), 2, '0') || ':' ||
	                LPAD(TO_CHAR(itmv.DH_MVTO_ESTOQUE, 'SS'), 2, '0'),
	                'YYYY-MM-DD HH24:MI:SS'
	            ) 
	        ELSE 
	            itmv.DH_MVTO_ESTOQUE
	    END AS DH_MVTO_ESTOQUE,
	    itmv.QT_MOVIMENTACAO,
	    itmv.VL_UNITARIO,
	    itmv.TP_ESTOQUE
	FROM DBAMV.ITMVTO_ESTOQUE itmv
	WHERE itmv.CD_ITMVTO_ESTOQUE >= :PARAM
)
SELECT * FROM itmvto_estoquee