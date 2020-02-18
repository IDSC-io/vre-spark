SELECT	[TERMINID]
		,FACT_TERMINRAUM.[RAUMID]
		,DIM_RAUM.RAUMNAME
		,[TERMINSTART_TS]
		,[TERMINENDE_TS]
		,[DAUERINMIN]
FROM [Atelier_DataScience].[atl].[V_DH_FACT_TERMINRAUM] as FACT_TERMINRAUM
	LEFT JOIN [Atelier_DataScience].[atl].V_DH_DIM_RAUM_CUR as DIM_RAUM
	ON FACT_TERMINRAUM.RAUMID = DIM_RAUM.RAUMID
WHERE DIM_RAUM.RAUMNAME IS NOT NULL