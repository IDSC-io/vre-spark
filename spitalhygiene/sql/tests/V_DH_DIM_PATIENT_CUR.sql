SELECT
    [PATIENTID]
	,[GESCHLECHT]	=	CASE GSCHL
							WHEN 1 THEN 'männlich'
							WHEN 2 THEN  'weiblich'
							ELSE 'Unbekannt'
						END
	,[GEBURTSDATUM] = GBDAT
    ,[PLZ]			= PSTLZ
    ,[WOHNORT]		= ORT
    ,[KANTON]		= BLAND
    ,[SPRACHE]		= SPRAS_TEXT
FROM [Atelier_DataScience].[atl].[V_LA_ISH_NPAT_NORM]
WHERE PATIENTID IN ('00003067149', '00008301433', '00004348346')

