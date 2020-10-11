USE cdwh_PROD


-- In this query only prescriptions/feedbacks of antibiotics (ATC J01*) are considered (OS: Ipdos).
-- for each patient, the time of initial record of a prescription as well as any initiation, pause, modification or termination is considered

SELECT DISTINCT 
	c.[pid],
	verordnung_id			= ao.ORDERID,
	verordnung_datum		= CONVERT(DATE,ao.ERFDAT),
	medikament_name			= dm.dim_medikamente,
	medikament_atc			= dm.dim_atc_bk,
	aktionstyp				= CASE 
								WHEN aa.AKTION = 'a' THEN 'Rueckmeldung'
								WHEN aa.AKTION = 'b' THEN 'Start'
								WHEN aa.AKTION = 'm' THEN 'Modifikation'
								WHEN aa.AKTION = 'p' THEN 'Pause_Start'
								WHEN aa.AKTION = 'r' THEN 'Pause_Ende'
								WHEN aa.AKTION = 's' THEN 'Ende'
							END,
	aktion_zeitpunkt		= CASE
								WHEN aa.time1 IS NOT NULL THEN CONVERT(SMALLDATETIME,aa.time1)
								ELSE CONVERT(SMALLDATETIME,aa.AKTIONSDATUM)
							END 

FROM [AtelierIDCL].[dbo].[20201011173131_patient_ids] AS c

LEFT JOIN rep.v_dim_patient AS p
ON c.pid = p.dim_patient_pid_int

LEFT JOIN [dbo].[LA_KIS_PATIENTID] AS pat
ON p.dim_patient_bk = pat.IDP_PATIENTID

LEFT JOIN [dbo].[LA_KIS_APPDRUGORDER] AS ao
ON pat.PATID = ao.PATID
AND ao.ORDERID > 0
AND ao.CODE IN (SELECT [dim_medikamente_bk] FROM [rep].[v_dim_medikamente] WHERE dim_atc_bk LIKE 'J01%')

LEFT JOIN [dbo].[LA_KIS_APPDRUGACTION] AS aa
ON ao.ORDERID = aa.ORDERID
AND aa.AKTION NOT IN ('_a','_b','_m','_p','_r','_s','a')

LEFT JOIN [rep].[v_dim_medikamente] AS dm
ON ao.CODE = dm.dim_medikamente_bk

ORDER BY 1,4,3,7