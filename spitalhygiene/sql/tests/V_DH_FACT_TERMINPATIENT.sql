SELECT	[TERMINID]
		,[PATIENTID]
		,[FALLID]
FROM [Atelier_DataScience].[atl].[V_DH_FACT_TERMINPATIENT]
WHERE PATIENTID in ('00003067149', '00008301433', '00004348346')
