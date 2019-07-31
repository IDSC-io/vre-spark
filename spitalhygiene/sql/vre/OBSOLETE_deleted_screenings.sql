SELECT
        NPAT.VNAME
        , NPAT.NNAME
        , NPAT.PATIENTID
        , NPAT.GBDAT
        , CASE WHEN len(Screening1Datum)>1 THEN CONVERT(date, Screening1Datum, 104) END AS ScreeningDate
  FROM [Atelier_DataScience].[dbo].[deleted_screenings] as D
  LEFT OUTER JOIN [Atelier_DataScience].[atl].[V_LA_ISH_NPAT_NORM] as NPAT ON
  D.Name = NPAT.NNAME AND D.Vorname = NPAT.VNAME AND CONVERT(date, DatumGeboren, 104) = NPAT.GBDAT