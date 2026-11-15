-- ============================================
-- Load source data from CSVs into memory.main
-- ============================================

SET VARIABLE URL = 'https://raw.githubusercontent.com/chop-analytics/analyst-take-home-task/master/datasets';

DROP TABLE IF EXISTS patients;   CREATE TABLE patients    AS SELECT * FROM read_csv_auto(getvariable('URL')||'/'||'patients.csv'   , SAMPLE_SIZE = -1);
DROP TABLE IF EXISTS encounters; CREATE TABLE encounters  AS SELECT * FROM read_csv_auto(getvariable('URL')||'/'||'encounters.csv' , SAMPLE_SIZE = -1);
DROP TABLE IF EXISTS medications;CREATE TABLE medications AS SELECT * FROM read_csv_auto(getvariable('URL')||'/'||'medications.csv', SAMPLE_SIZE = -1);
DROP TABLE IF EXISTS procedures; CREATE TABLE procedures  AS SELECT * FROM read_csv_auto(getvariable('URL')||'/'||'procedures.csv' , SAMPLE_SIZE = -1);
DROP TABLE IF EXISTS allergies;  CREATE TABLE allergies   AS SELECT * FROM read_csv_auto(getvariable('URL')||'/'||'allergies.csv'  , SAMPLE_SIZE = -1);

-- ==================================================
-- Create final solution table and export to CSV file
-- ==================================================

WITH  X AS (SELECT
                              m.PATIENT AS PATIENT_ID,
                              e.ID AS ENCOUNTER_ID,
                              MAX(CASE WHEN LOWER(m.description) ILIKE '%hydromorphone%325%mg' 
                                      OR LOWER(m.description) ILIKE '%fentanyl%100%mcg' 
                                      OR LOWER(m.description) ILIKE '%oxycodone%acetaminophen%100%ml' THEN 1 ELSE 0 END) AS CURRENT_OPIOID_IND
                                      
                           ,  MAX(CASE WHEN m.code IN (316049,429503,406022)                             THEN 1 ELSE 0 END) AS CURRENT_OPIOID_IND_M
                          FROM encounters e JOIN medications m ON m.PATIENT = e.PATIENT  WHERE m.START <= e.START AND (m.STOP = 'NA' OR CAST(m.STOP AS DATE) >= e.START) GROUP BY m.PATIENT, e.ID
             )
      SELECT SUM(CURRENT_OPIOID_IND), SUM(CURRENT_OPIOID_IND_M) FROM X


CREATE OR REPLACE VIEW solution AS 
(
WITH opioid_indicators AS (
                         SELECT
                              m.PATIENT AS PATIENT_ID,
                              e.ID AS ENCOUNTER_ID,
                              MAX(CASE WHEN LOWER(m.description) ILIKE '%hydromorphone%325%mg' 
                                      OR LOWER(m.description) ILIKE '%fentanyl%100%mcg' 
                                      OR LOWER(m.description) ILIKE '%oxycodone%acetaminophen%100%ml' THEN 1 ELSE 0 END) AS CURRENT_OPIOID_IND
                                      
                           ,  MAX(CASE WHEN m.code IN (316049,429503,406022)                             THEN 1 ELSE 0 END) AS CURRENT_OPIOID_IND_M
                          FROM encounters e JOIN medications m ON m.PATIENT = e.PATIENT  WHERE m.START <= e.START AND (m.STOP = 'NA' OR CAST(m.STOP AS DATE) >= e.START) GROUP BY m.PATIENT, e.ID
                             ),

medication_counts AS (
                          SELECT m.PATIENT AS PATIENT_ID,
                                  e.ID      AS ENCOUNTER_ID,
                                  COUNT(*)  AS COUNT_CURRENT_MEDS
                           FROM encounters e JOIN medications m ON m.PATIENT = e.PATIENT   WHERE m.START <= e.START AND (m.STOP = 'NA' OR CAST(m.STOP AS DATE) >= e.START) GROUP BY m.PATIENT, e.ID
                        ),

readmissions AS (
                SELECT
                     e1.ID             AS ENCOUNTER_ID,
                     e1.PATIENT        AS PATIENT_ID,
                     CASE WHEN EXISTS ( SELECT 1             FROM encounters e2 WHERE e2.PATIENT = e1.PATIENT AND e2.START > e1.STOP AND e2.START <= e1.STOP + INTERVAL '90 DAYS' AND LOWER(e2.REASONDESCRIPTION) = 'drug overdose') THEN 1 ELSE 0 END AS READMISSION_90_DAY_IND,
                     CASE WHEN EXISTS ( SELECT 1             FROM encounters e2 WHERE e2.PATIENT = e1.PATIENT AND e2.START > e1.STOP AND e2.START <= e1.STOP + INTERVAL '30 DAYS' AND LOWER(e2.REASONDESCRIPTION) = 'drug overdose') THEN 1 ELSE 0 END AS READMISSION_30_DAY_IND,
                                      ( SELECT MIN(e2.START) FROM encounters e2 WHERE e2.PATIENT = e1.PATIENT AND e2.START > e1.STOP AND e2.START <= e1.STOP + INTERVAL '90 DAYS' AND LOWER(e2.REASONDESCRIPTION) = 'drug overdose'                  ) AS FIRST_READMISSION_DATE
                   FROM encounters e1
                  )

SELECT DISTINCT
               patients.ID AS PATIENT_ID,
               encounters.ID AS ENCOUNTER_ID,
               encounters.START AS HOSPITAL_ENCOUNTER_DATE,
               DATE_PART('YEAR', AGE(encounters.START, patients.BIRTHDATE)) AS AGE_AT_VISIT,
               CASE
               	WHEN CAST(REPLACE(patients.DEATHDATE, 'NA', '9999-12-31') AS DATE) BETWEEN CAST(ENCOUNTERS.START AS DATE) AND CAST(ENCOUNTERS.STOP AS DATE)
               	THEN 1
               	ELSE 0
               END AS DEATH_AT_VISIT_IND,
               COALESCE(mc.COUNT_CURRENT_MEDS, 0) AS COUNT_CURRENT_MEDS,
               COALESCE(oi.CURRENT_OPIOID_IND, 0) AS CURRENT_OPIOID_IND,
               COALESCE(r.READMISSION_90_DAY_IND, 0) AS READMISSION_90_DAY_IND,
               COALESCE(r.READMISSION_30_DAY_IND, 0) AS READMISSION_30_DAY_IND,
               COALESCE(STRFTIME(r.FIRST_READMISSION_DATE, '%Y-%m-%d'), 'N/A') AS FIRST_READMISSION_DATE
         FROM encounters
         JOIN patients              ON encounters.PATIENT = patients.ID
    LEFT JOIN medication_counts mc  ON mc.PATIENT_ID      = patients.ID AND mc.ENCOUNTER_ID = encounters.ID
    LEFT JOIN opioid_indicators oi  ON oi.PATIENT_ID      = patients.ID AND oi.ENCOUNTER_ID = encounters.ID
    LEFT JOIN readmissions r        ON  r.PATIENT_ID      = patients.ID AND r.ENCOUNTER_ID = encounters.ID
        WHERE 1=1
       -- AND LOWER(encounters.REASONDESCRIPTION) = 'drug overdose' 
          AND encounters.reasoncode IN ('55680006') 
          AND encounters.START > '1999-07-15' 
          AND (DATE_PART('YEAR', AGE(encounters.START, patients.BIRTHDATE)) BETWEEN 18 AND 35)
);


--COPY solution TO 'C:/Users/nicho/Documents/Vanguard Assessment/solution_output.csv' (HEADER, DELIMITER ',');


-- ==================================
-- Find answers to business questions
-- ==================================

          SELECT 'ENCOUNTERS'             , COUNT(*)                   AS TOTAL_PATIENT_ENCOUNTERS                  FROM solution
UNION ALL SELECT 'PATIENTS'               , COUNT(DISTINCT PATIENT_ID) AS DISTINCT_PATIENTS                         FROM solution
UNION ALL SELECT 'ACTIVE OP RX ENCOUNTERS', COUNT(*)                   AS NUM_ACTIVE_OPIOID_ENCOUNTERS              FROM solution WHERE CURRENT_OPIOID_IND     = 1
UNION ALL SELECT 'DEATHS'                 , COUNT(*)                   AS NUM_DEATHS_DURING_DRUG_OVERDOSE_ENCOUNTER FROM solution WHERE DEATH_AT_VISIT_IND     = 1
UNION ALL SELECT 'READM 90D'              , COUNT(*)                   AS READMISSIONS_WITHIN_90_DAYS               FROM solution WHERE READMISSION_90_DAY_IND = 1
UNION ALL SELECT 'READM 30D'              , COUNT(*)                   AS READMISSIONS_WITHIN_30_DAYS               FROM solution WHERE READMISSION_30_DAY_IND = 1