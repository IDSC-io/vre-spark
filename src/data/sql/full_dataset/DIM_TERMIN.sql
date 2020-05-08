SELECT	[TERMINID]              = dim_termin_bk
		,[IS_DELETED]           = is_deleted
		,[TERMINBEZEICHNUNG]    = dim_termin_terminbezeichnung
		,[TERMINART]            = dim_termin_terminart
		,[TERMINTYP]            = dim_termin_termintyp
		,[TERMINDATUM]          = dim_termin_termindatum
		,[DAUERINMIN]           = dim_termin_dauer_in_min
FROM [Atelier_DataScience].[atl].[dim_termin]

