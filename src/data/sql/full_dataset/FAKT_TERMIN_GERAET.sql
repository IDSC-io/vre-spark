SELECT	[TERMINID]          = dim_termin_bk
		,[GERAETID]         = dim_geraet_bk
		,[TERMINSTART_TS]   = fakt_termin_geraet_start
		,[TERMINENDE_TS]    = fakt_termin_geraet_ende
		,[DAUERINMIN]       = fakt_termin_geraet_dauerinmin
FROM [Atelier_DataScience].[atl].[fakt_termin_geraet]

