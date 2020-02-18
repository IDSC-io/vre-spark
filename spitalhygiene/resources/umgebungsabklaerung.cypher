MATCH (n:Patient{PATNR:'00000532290'})-[k]-(m:Patient) 
WHERE k.from > n.risk_datetime - duration('P8D')
AND n.PATNR <> m.PATNR
RETURN n.PATNR, 
 m.PATNR, 
 k.from AS von, 
 k.to AS bis, 
 k.room AS ort, 
 CASE type(k) WHEN 'kontakt_raum' THEN 'Zimmer' WHEN 'kontakt_org' THEN 'Abteilung' END AS typ, 
 CASE "True" IN labels(m) WHEN true THEN 'Ja' WHEN false THEN 'Nein' END AS infiziert
