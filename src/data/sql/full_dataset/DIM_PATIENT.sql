SELECT
    [PATIENTID]     = dim_patient_bk
	,[GESCHLECHT]	= dim_patient_geschlecht
	,[GEBURTSDATUM] = dim_patient_geburtsdatum
    ,[PLZ]			= dim_patient_plz
    ,[WOHNORT]		= dim_patient_wohnort
    ,[KANTON]		= dim_patient_kanton
    ,[SPRACHE]		= dim_patient_sprache
FROM [Atelier_DataScience].[atl].[dim_patient]

