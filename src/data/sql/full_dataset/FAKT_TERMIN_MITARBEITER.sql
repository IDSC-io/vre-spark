SELECT	[TERMINID]          = dim_termin_bk
		,[MITARBEITERID]    = dim_mitarbeiter_bk
		,[TERMINSTART_TS]   = fakt_termin_mitarbeiter_start
		,[TERMINENDE_TS]    = fakt_termin_mitarbeiter_ende
		,[DAUERINMIN]       = fakt_termin_mitarbeiter_dauerinmin
FROM [Atelier_DataScience].[atl].[fakt_termin_mitarbeiter]

