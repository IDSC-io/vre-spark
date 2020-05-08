SELECT
    CHOPCODE                = ZCODE
    ,CHOPVERWENDUNGSJAHR    = VERWENDUNGSJAHR
    ,CHOP                   = TEXT
    ,CHOPCODELEVEL1         = ZCODE_LEVEL1
    ,CHOPLEVEL1             = TEXT_LEVEL1
    ,CHOPCODELEVEL2         = ZCODE_LEVEL2
    ,CHOPLEVEL2             = TEXT_LEVEL2
    ,CHOPCODELEVEL3         = ZCODE_LEVEL3
    ,CHOPLEVEL3             = TEXT_LEVEL3
    ,CHOPCODELEVEL4         = ZCODE_LEVEL4
    ,CHOPLEVEL4             = TEXT_LEVEL4
    ,CHOPCODELEVEL5         = ZCODE_LEVEL5
    ,CHOPLEVEL5             = TEXT_LEVEL5
    ,CHOPCODELEVEL6         = ZCODE_LEVEL6
    ,CHOPLEVEL6             = TEXT_LEVEL6
    ,CHOPSTATUS             = STATUS
    ,CHOPSAPKATALOGID       = SAP_KATALOGID
FROM [Atelier_DataScience].[dbo].[LA_CHOP_FLAT]
WHERE ZCODE in (
					'Z99.B8.11'
					,'Z50.27.32'
					,'Z00.99.60'
					,'Z88.38.60'
					,'Z99.85'
					,'Z89.07.24'
					,'Z50.27.32'
					,'Z00.99.60'
					,'Z88.38.60'
					,'Z99.85'
					,'Z50.23.13'
					,'Z50.12.09'
					,'Z88.79.50'
					,'Z00.9A.13'
					,'Z39.32.41'
					,'Z50.93'
					,'Z34.84'
					,'Z34.89.99'
					,'Z39.29.89'
					,'Z50.52'
					,'Z00.93.99'
					,'Z00.90.99'
					,'Z00.99.10'
					,'Z50.27.32'
					,'Z00.99.60'
					,'Z88.38.60'
					,'Z99.85'
					,'Z99.04.10'
					,'Z94.8X.40'
					,'Z99.B7.12'
					,'Z99.07.3C'
					,'Z99.04.15'
					,'Z99.05.47'
					,'Z99.B7.13'
					,'Z99.0A'
					,'Z99.28.11'
					,'Z50.52'
					,'Z54.52'
					,'Z00.93.99'
					,'Z00.90.99'
					,'Z51.22.11'
					,'Z39.29.89'
					,'Z99.00'
					,'Z54.12.11'
					,'Z50.12.12'
					,'Z88.79.50'
					,'Z54.25'
					,'Z36.11.22'
					,'Z36.11.26'
					,'Z36.1C.12'
					,'Z39.61.10'
					,'Z39.63'
					,'Z39.64'
					,'Z88.79.50'
					,'Z01.16.12'
					,'Z99.00'
					)
