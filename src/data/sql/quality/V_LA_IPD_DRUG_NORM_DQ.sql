SELECT
    sum (case when [PATIENTID] is null then 0 else 1 end) as "PATIENTID_NONNULLCOUNT",
    sum (case when [PATIENTID] is null then 1 else 0 end) as "PATIENTID_NULLCOUNT",
    sum (case when [CASEID] is null then 0 else 1 end) as "CASEID_NONNULLCOUNT",
    sum (case when [CASEID] is null then 1 else 0 end) as "CASEID_NULLCOUNT",
    sum (case when [DRUG_TEXT] is null then 0 else 1 end) as "DRUG_TEXT_NONNULLCOUNT",
    sum (case when [DRUG_TEXT] is null then 1 else 0 end) as "DRUG_TEXT_NULLCOUNT",
    sum (case when [DRUG_ATC] is null then 0 else 1 end) as "DRUG_ATC_NONNULLCOUNT",
    sum (case when [DRUG_ATC] is null then 1 else 0 end) as "DRUG_ATC_NULLCOUNT",
    sum (case when [DRUG_QUANTITY] is null then 0 else 1 end) as "DRUG_QUANTITY_NONNULLCOUNT",
    sum (case when [DRUG_QUANTITY] is null then 1 else 0 end) as "DRUG_QUANTITY_NULLCOUNT",
    sum (case when [DRUG_UNIT] is null then 0 else 1 end) as "DRUG_UNIT_NONNULLCOUNT",
    sum (case when [DRUG_UNIT] is null then 1 else 0 end) as "DRUG_UNIT_NULLCOUNT",
    sum (case when [DRUG_DISPFORM] is null then 0 else 1 end) as "DRUG_DISPFORM_NONNULLCOUNT",
    sum (case when [DRUG_DISPFORM] is null then 1 else 0 end) as "DRUG_DISPFORM_NULLCOUNT",
    sum (case when [DRUG_SUBMISSION] is null then 0 else 1 end) as "DRUG_SUBMISSION_NONNULLCOUNT",
    sum (case when [DRUG_SUBMISSION] is null then 1 else 0 end) as "DRUG_SUBMISSION_NULLCOUNT"
FROM [Atelier_DataScience].[atl].[V_LA_IPD_DRUG_NORM]