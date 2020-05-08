SELECT	[TERMINID]          = dim_termin_bk
        ,[RAUMID]           = dim_raum_bk
		,RAUMNAME           = dim_raum_name
		,[TERMINSTART_TS]   = fakt_termin_raum_start
		,[TERMINENDE_TS]    = fakt_termin_raum_ende
		,[DAUERINMIN]       = fakt_termin_raum_dauerinmin
FROM [Atelier_DataScience].[atl].[fakt_termin_raum]
WHERE dim_termin_bk IN (
                        '38515699'
                        ,'38321122'
                        ,'35416924'
                        ,'1164130 '
                        ,'38470639'
                        ,'41827160'
                        ,'39893063'
                        ,'38411180'
                        ,'35571391'
                        ,'35130813'
                        ,'36160483'
                        ,'40766840'
                        ,'42155710'
                        ,'39491988'
                        ,'36067632'
                        ,'37374631'
                        ,'36129549'
                        ,'39001478'
                        ,'39425469'
                        ,'34338471'
                        ,'35630084'
                        ,'35139096'
                        ,'38431954'
                        ,'38452040'
                        ,'40344805'
                        ,'13831398'
                        ,'38063644'
                        ,'38539785'
                        ,'34220024'
                        ,'39819467'
                        ,'39423020'
                        ,'38386995'
                        ,'42394432'
                        ,'38446243'
                        ,'42213628'
                        ,'38565198'
                        ,'39893320'
                        ,'37244357'
                        ,'37554138'
                        ,'41124954'
                        ,'39051017'
                        ,'36129560'
                        ,'35621237'
                        ,'38772701'
                        ,'21130116'
                        ,'38063650'
                        ,'39608858'
                        ,'39427731'
                        ,'21131159'
                        ,'38331618'
                        ,'38062724'
                        ,'24171386'
                        ,'14908956'
                        ,'41909560'
                        ,'39114133'
                        ,'14091256'
                        ,'38939623'
                        ,'35626775'
                        ,'35139491'
                        ,'36006751'
                        ,'38329080'
                        ,'41909690'
                        ,'35130747'
                        ,'36129541'
                        ,'1278803 '
                        ,'38507433'
                        ,'1192059 '
                        ,'39456191'
                        ,'14091249'
                        ,'39933520'
                        ,'24291359'
                        ,'36071093'
                        ,'36160474'
                        ,'19096210'
                        ,'40218521'
                        ,'1162144 '
                        ,'38660148'
                        ,'42211133'
                        ,'39613790'
                        ,'24230235'
                        ,'38262758'
                        ,'35417252'
                        ,'19252406'
                        ,'39215737'
                        ,'38446041'
                        ,'36830543'
                        ,'35200182'
                        ,'40766156'
                        ,'36070942'
                        ,'34310589'
                        ,'37232112'
                        ,'34337667'
                        ,'38446523'
                        ,'34482529'
                        ,'17297480'
                        ,'39298995'
                        ,'36830574'
                        ,'1405150'
                        )
