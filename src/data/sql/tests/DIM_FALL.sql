SELECT
    [PATIENTID]         = dim_fall_patient_bk
    ,[CASEID]           = dim_fall_bk
    ,[CASETYP]          = dim_fall_fallart_cd
    ,[CASESTATUS]       = dim_fall_status
    ,[FALAR]            = dim_fall_fallart
    ,[BEGDT]            = dim_fall_start_datum
    ,[ENDDT]            = dim_fall_ende_datum
    ,[PATIENTTYP]       = dim_patient_typ
    ,[PATIENTSTATUS]    = dim_patient_status
FROM [Atelier_DataScience].[atl].[dim_fall]
WHERE dim_fall_patient_bk IN ('00003067149', '00008301433', '00004348346')
