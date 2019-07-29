SELECT
    sum (case when [PATCASEID] is null then 0 else 1 end) as "PATCASEID_NONNULLCOUNT",
    sum (case when [PATCASEID] is null then 1 else 0 end) as "PATCASEID_NULLCOUNT",
    sum (case when [COST_WEIGHT] is null then 0 else 1 end) as "COST_WEIGHT_NONNULLCOUNT",
    sum (case when [COST_WEIGHT] is null then 1 else 0 end) as "COST_WEIGHT_NULLCOUNT"
FROM [Atelier_DataScience].[atl].[LA_ISH_NDRG]
