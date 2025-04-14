/*
TOOLS
   sourcetree
   DBeaver
   
SETUP
   Fork   repo - https://github.com/chop-analytics/analyst-take-home-task
   Forked repo - clone to local
   remove ".x" from procedures.csv column names
   
NOTES
  EXECUTE IMMEDIATE not available for CREATE TABLE DDL.  For full automation, use python with duckdb lib
  All objects created in memory.main db schema
*/


SET VARIABLE dpath = 'C:\github\chop-test\datasets';

DROP TABLE IF EXISTS allergies;
DROP TABLE IF EXISTS encounters;
DROP TABLE IF EXISTS medications;
DROP TABLE IF EXISTS patients;
DROP TABLE IF EXISTS procedures;

SET VARIABLE tab = 'patients'   ;  CREATE TABLE IF NOT EXISTS patients    AS SELECT * FROM read_csv_auto(getvariable('dpath')||'\'||getvariable('tab')||'.csv', SAMPLE_SIZE = -1);
SET VARIABLE tab = 'encounters' ;  CREATE TABLE IF NOT EXISTS encounters  AS SELECT * FROM read_csv_auto(getvariable('dpath')||'\'||getvariable('tab')||'.csv', SAMPLE_SIZE = -1);
SET VARIABLE tab = 'allergies'  ;  CREATE TABLE IF NOT EXISTS allergies   AS SELECT * FROM read_csv_auto(getvariable('dpath')||'\'||getvariable('tab')||'.csv', SAMPLE_SIZE = -1);
SET VARIABLE tab = 'medications';  CREATE TABLE IF NOT EXISTS medications AS SELECT * FROM read_csv_auto(getvariable('dpath')||'\'||getvariable('tab')||'.csv', SAMPLE_SIZE = -1);
SET VARIABLE tab = 'procedures' ;  CREATE TABLE IF NOT EXISTS procedures  AS SELECT * FROM read_csv_auto(getvariable('dpath')||'\'||getvariable('tab')||'.csv', SAMPLE_SIZE = -1);

  ALTER TABLE patients    ADD CONSTRAINT patients_pk   PRIMARY KEY (Id);
  ALTER TABLE encounters  ADD CONSTRAINT encounters_pk PRIMARY KEY (Id);
--ALTER TABLE encounters  ADD CONSTRAINT encounters_patients_FK FOREIGN KEY (patient) REFERENCES patients(Id);   --not enabled in DuckDB outside create statement


  

/* Assemble project cohort 

identify patients seen for drug OD - 
Determine active opioid at encounter start
SHOW drug OD readmissions count - Hosptial readmission = 32485007

cohort : encounters that meet
   patient’s visit is encounter for drug OD
   hospital encounter > 1999-07-15
   patient age (at time of encounter) BETWEEN 18 and 35 ( >=18 AND < 36)

Opioids List (RxNORM)
   316049 - Hydromorphone 325Mg
   429503 - Fentanyl – 100 MCG
   406022 - Oxycodone-acetaminophen 100 Ml

*/


CREATE OR REPLACE VIEW overdose_cohort 
AS
WITH patient_med_list AS ( SELECT e.patient                                                         AS patient ,
                                  e.id                                                              AS encounter ,
                                  COUNT(m.code)                                                     AS count_current_meds,
                                  MAX(CASE WHEN m.code IN (316049,429503,406022) THEN 1 ELSE 0 END) AS CURRENT_OPIOID_IND
                             FROM encounters  e
                             JOIN medications m ON e.patient = m.patient
                                               AND e.start BETWEEN m.start AND (CASE m.stop WHEN 'NA' THEN CURRENT_DATE ELSE m.stop::DATE END)
                            GROUP BY e.patient, e.id
                            ORDER BY e.patient, e.id)
  SELECT   pat.last                                                                             AS PATIENT_NAME            ,
           pat.id                                                                               AS PATIENT_ID              ,
           ROW_NUMBER() OVER (PARTITION BY pat.id ORDER BY enc.START )                          AS seq                     ,
         --enc.id                                                                               AS ENCOUNTER_ID            ,
           DATE_DIFF('year',pat.BIRTHDATE, enc.START)                                           AS AGE_AT_VISIT            ,
           enc.START                                                                            AS HOSPITAL_ENCOUNTER_DATE ,
       LAG(enc.START ) OVER (PARTITION BY pat.id ORDER BY enc.START)                            AS PRIOR_ADMISSION,
         --STRFTIME(enc.STOP ::DATE,'%Y-%m-%d')                                                 AS ENCOUNTER_STOP          ,
           CASE pat.DEATHDATE WHEN 'NA' THEN '' ELSE pat.DEATHDATE END                          AS DEATH_DATE              ,
           CASE WHEN pat.DEATHDATE = STRFTIME(enc.STOP ::DATE,'%Y-%m-%d') THEN 1                
                WHEN pat.DEATHDATE <> 'NA'                                THEN 0                
                ELSE NULL                                                                       
                END                                                                             AS DEATH_AT_VISIT_IND ,
                                                                                                
           pml.COUNT_CURRENT_MEDS                                                               AS COUNT_CURRENT_MEDS ,
           pml.CURRENT_OPIOID_IND                                                               AS CURRENT_OPIOID_IND ,
          -- enc.ENCOUNTERCLASS, 
          -- enc.DESCRIPTION, 
          -- enc.REASONDESCRIPTION

      FROM patients         pat 
      JOIN encounters       enc ON pat.id      = enc.patient
 LEFT JOIN patient_med_list pml ON enc.patient = pml.patient 
                               AND enc.id      = pml.encounter
     WHERE enc.reasoncode IN ('55680006')                                      --SNOMED-CT overdose
       AND DATE_DIFF('year', pat.BIRTHDATE, enc.START) BETWEEN 18 and 35       --age requirement
       AND enc.START > '1999-07-15'                                            --temporal requirement
     ORDER BY pat.id,  --ORDER BY is for debugging, possibly remove for performance
              enc.start
      ;
      
 
-- 432 rows      
      
WITH FIRST_REMISSION AS (SELECT patient_id, hospital_encounter_date AS FIRST_READMISSION_DATE  FROM overdose_cohort where seq = 2)      
SELECT --PATIENT_NAME, 
       oc.PATIENT_ID, 
       AGE_AT_VISIT, 
       HOSPITAL_ENCOUNTER_DATE,
       PRIOR_ADMISSION,
       DEATH_DATE, 
       DEATH_AT_VISIT_IND, 
       COUNT_CURRENT_MEDS, 
       CURRENT_OPIOID_IND,
       DATEDIFF('day', PRIOR_ADMISSION, HOSPITAL_ENCOUNTER_DATE)  AS DIFF,
       CASE WHEN DATEDIFF('day', PRIOR_ADMISSION, HOSPITAL_ENCOUNTER_DATE) <= 90 THEN 1 ELSE 0 END AS READMISSION_90_DAY_IND,
       CASE WHEN DATEDIFF('day', PRIOR_ADMISSION, HOSPITAL_ENCOUNTER_DATE) <= 30 THEN 1 ELSE 0 END AS READMISSION_30_DAY_IND,
       fr.FIRST_READMISSION_DATE
       
     FROM overdose_cohort oc
LEFT JOIN FIRST_REMISSION fr ON oc.patient_id = fr.patient_id
--where READMISSION_90_DAY_IND <> READMISSION_30_DAY_IND





