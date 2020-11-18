USE cdwh_PROD


-- In this query only prescriptions/feedbacks of antibiotics (ATC J01*) are considered (OS: Ipdos).
-- For each prescription, the date of the first and last dispense/feedback with dispense amount > 0 is considered

;WITH mf_cte (
			pid,
			verordnungsid,
			medikament,
			atc,
			zeitpunkt_rueckgemeldet,
			seq_asc,
			seq_desc
			) 
AS (
	SELECT DISTINCT
		 k.pid
		,verordnungsid				= mf.fakt_medikamente_aktion_verordnungsid
		,medikament					= md.dim_medikamente
		,atc						= md.dim_atc_bk
		,zeitpunkt_rueckgemeldet	= mf.fakt_medikamente_aktion_abgabe_verabreicht_datumzeit
		,seq_asc					= ROW_NUMBER() OVER(PARTITION BY k.pid, mf.fakt_medikamente_aktion_verordnungsid ORDER BY mf.fakt_medikamente_aktion_abgabe_verabreicht_datumzeit ASC)
		,seq_desc					= ROW_NUMBER() OVER(PARTITION BY k.pid, mf.fakt_medikamente_aktion_verordnungsid ORDER BY mf.fakt_medikamente_aktion_abgabe_verabreicht_datumzeit DESC)

	FROM [AtelierIDCL].[dbo].[20201011173131_patient_ids] AS k

	JOIN rep.v_dim_patient AS p
	ON k.pid = p.dim_patient_pid_int

	JOIN rep.[v_fakt_medikamente_aktion] AS mf 
	ON p.dim_patient_sid = mf.dim_patient_sid
	AND mf.fakt_medikamente_aktion_anzahl_verabreicht > 0

	JOIN rep.v_dim_medikamente AS md
	ON mf.dim_medikamente_sid = md.dim_medikamente_sid
	AND md.dim_atc_bk LIKE 'J01%'
	)

SELECT 
	k.pid,
	mf_s.verordnungsid,
	mf_s.medikament,
	mf_s.atc,
	therapie_start		= CONVERT(SMALLDATETIME,mf_s.zeitpunkt_rueckgemeldet),
	therapie_ende		= CONVERT(SMALLDATETIME,mf_e.zeitpunkt_rueckgemeldet)

FROM [AtelierIDCL].[dbo].[20201011173131_patient_ids] AS k

LEFT JOIN mf_cte AS mf_s
ON k.pid = mf_s.pid
AND mf_s.seq_asc = 1

LEFT JOIN mf_cte AS mf_e
ON k.pid = mf_e.pid
AND mf_s.verordnungsid = mf_e.verordnungsid
AND mf_e.seq_desc = 1

ORDER BY 1,2