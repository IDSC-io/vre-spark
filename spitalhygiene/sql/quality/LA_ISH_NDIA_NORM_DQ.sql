SELECT
	sum (case when [FALNR] is null then 0 else 1 end) as "FALNR_NONNULLCOUNT",
	sum (case when [FALNR] is null then 1 else 0 end) as "FALNR_NULLCOUNT",
	sum (case when [DKEY1] is null then 0 else 1 end) as "DKEY1_NONNULLCOUNT",
	sum (case when [DKEY1] is null then 1 else 0 end) as "DKEY1_NULLCOUNT",
	sum (case when [DKAT1] is null then 0 else 1 end) as "DKAT1_NONNULLCOUNT",
	sum (case when [DKAT1] is null then 1 else 0 end) as "DKAT1_NULLCOUNT",
	sum (case when [DIADT] is null then 0 else 1 end) as "DIADT_NONNULLCOUNT",
	sum (case when [DIADT] is null then 1 else 0 end) as "DIADT_NULLCOUNT",
	sum (case when [DRG_CATEGORY] is null then 0 else 1 end) as "DRG_CATEGORY_NONNULLCOUNT",
	sum (case when [DRG_CATEGORY] is null then 1 else 0 end) as "DRG_CATEGORY_NULLCOUNT"
FROM [Atelier_DataScience].[atl].[LA_ISH_NDIA_NORM]
WHERE DKEY1 is not NULL and DRG_CATEGORY is not null
