--SAMPLE QUERIES

-- Patient encounters
  SELECT 
          p.id                           AS patient_id,
          COUNT(DISTINCT e.id)           AS encounter_count,
          COUNT(         e.id)           AS test_count,
          CASE WHEN COUNT(DISTINCT e.id) <> COUNT(e.id) THEN 1 ELSE 0 END AS check
    FROM  patients p JOIN encounters e on p.id = e.patient
GROUP BY  p.id
--HAVING  CASE WHEN COUNT(DISTINCT e.id) <> COUNT(e.id) THEN 1 ELSE 0 END > 0


