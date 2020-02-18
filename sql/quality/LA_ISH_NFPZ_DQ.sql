SELECT
	sum (case when [EARZT] is null then 0 else 1 end) as "EARZT_NONNULLCOUNT",
	sum (case when [EARZT] is null then 1 else 0 end) as "EARZT_NULLCOUNT",
	sum (case when [FARZT] is null then 0 else 1 end) as "FARZT_NONNULLCOUNT",
	sum (case when [FARZT] is null then 1 else 0 end) as "FARZT_NULLCOUNT",
	sum (case when [FALNR] is null then 0 else 1 end) as "FALNR_NONNULLCOUNT",
	sum (case when [FALNR] is null then 1 else 0 end) as "FALNR_NULLCOUNT",
	sum (case when [LFDNR] is null then 0 else 1 end) as "LFDNR_NONNULLCOUNT",
	sum (case when [LFDNR] is null then 1 else 0 end) as "LFDNR_NULLCOUNT",
	sum (case when [PERNR] is null then 0 else 1 end) as "PERNR_NONNULLCOUNT",
	sum (case when [PERNR] is null then 1 else 0 end) as "PERNR_NULLCOUNT",
	sum (case when [STORN] is null then 0 else 1 end) as "STORN_NONNULLCOUNT",
	sum (case when [STORN] is null then 1 else 0 end) as "STORN_NULLCOUNT"
FROM [Atelier_DataScience].[atl].[LA_ISH_NFPZ]