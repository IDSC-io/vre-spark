SELECT
	FALNR,
    DKEY1,
    DKAT1,
    DIADT,
    DRG_CATEGORY
FROM [Atelier_DataScience].[dbo].[LA_ISH_NDIA]
WHERE DKEY1 is not NULL and DRG_CATEGORY is not null
