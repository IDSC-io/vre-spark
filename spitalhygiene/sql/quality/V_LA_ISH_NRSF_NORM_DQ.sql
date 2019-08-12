SELECT
    sum (case when [PATIENTID] is null then 0 else 1 end) as "PATIENTID_NONNULLCOUNT",
    sum (case when [PATIENTID] is null then 1 else 0 end) as "PATIENTID_NULLCOUNT",
    sum (case when [RSFNR] is null then 0 else 1 end) as "RSFNR_NONNULLCOUNT",
    sum (case when [RSFNR] is null then 1 else 0 end) as "RSFNR_NULLCOUNT",
    sum (case when [KZTXT] is null then 0 else 1 end) as "KZTXT_NONNULLCOUNT",
    sum (case when [KZTXT] is null then 1 else 0 end) as "KZTXT_NULLCOUNT",
    sum (case when [ERDAT] is null then 0 else 1 end) as "ERDAT_NONNULLCOUNT",
    sum (case when [ERDAT] is null then 1 else 0 end) as "ERDAT_NULLCOUNT",
    sum (case when [ERTIM] is null then 0 else 1 end) as "ERTIM_NONNULLCOUNT",
    sum (case when [ERTIM] is null then 1 else 0 end) as "ERTIM_NULLCOUNT"
FROM [Atelier_DataScience].[atl].[V_LA_ISH_NRSF_NORM]
WHERE isnull(LOEKZ,'') <> 'X'