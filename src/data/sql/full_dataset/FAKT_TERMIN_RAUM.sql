SELECT	[TERMINID]          = dim_termin_bk
        ,[RAUMID]           = dim_raum_bk
		,RAUMNAME           = dim_raum_name
		,[TERMINSTART_TS]   = fakt_termin_raum_start
		,[TERMINENDE_TS]    = fakt_termin_raum_ende
		,[DAUERINMIN]       = fakt_termin_raum_dauerinmin
FROM [Atelier_DataScience].[atl].[fakt_termin_raum]

