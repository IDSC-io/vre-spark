SELECT 
    [PATIENTID],
    [RSFNR],
    [KZTXT],
    [ERDAT],
    [ERTIM]
FROM [Atelier_DataScience].[atl].[V_LA_ISH_NRSF_NORM]
WHERE isnull(LOEKZ,'') <> 'X'