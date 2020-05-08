SELECT	[TERMINID]      = dim_termin_bk
		,[PATIENTID]    = dim_patient_bk
		,[FALLID]       = dim_fall_bk
FROM [Atelier_DataScience].[atl].[fakt_termin_patient]
WHERE dim_patient_bk in ('00003067149', '00008301433', '00004348346')
