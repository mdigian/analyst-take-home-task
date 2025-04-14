/*
===========================================================================================================
TOOLS/DB
   DBeaver
   
SETUP
   Fork   repo - https://github.com/chop-analytics/analyst-take-home-task
   Forked repo - clone to local (sourcetree for Git management)
   remove ".x" from procedures.csv column names
   SET VARIABLE dpath below to location of datasets
   
NOTES
  EXECUTE IMMEDIATE not available for CREATE TABLE DDL.  use python with duckdb lib for better automation
  memory.main db schema - default for duckdb
  
===========================================================================================================

COHORT - 
   hospital encounter > 1999-07-15
   patients seen for drug OD (SNOWMED-CT = 55680006)
   patient age (at time of encounter) BETWEEN 18 and 35
   
Points of Interest
  drug OD readmissions counts
  at encounter start, active opioids from RxNORM list:
     316049 - Hydromorphone 325Mg
     429503 - Fentanyl – 100 MCG
     406022 - Oxycodone-acetaminophen 100 Ml
===========================================================================================================
*/

DROP TABLE IF EXISTS allergies;
DROP TABLE IF EXISTS encounters;
DROP TABLE IF EXISTS medications;
DROP TABLE IF EXISTS patients;
DROP TABLE IF EXISTS procedures;

--URL METHOD
SET VARIABLE URL = 'https://raw.githubusercontent.com/chop-analytics/analyst-take-home-task/master/datasets';
CREATE TABLE IF NOT EXISTS patients    AS SELECT * FROM read_csv_auto(getvariable('URL')||'/'||'patients.csv'   , SAMPLE_SIZE = -1);
CREATE TABLE IF NOT EXISTS encounters  AS SELECT * FROM read_csv_auto(getvariable('URL')||'/'||'encounters.csv' , SAMPLE_SIZE = -1);
CREATE TABLE IF NOT EXISTS medications AS SELECT * FROM read_csv_auto(getvariable('URL')||'/'||'medications.csv', SAMPLE_SIZE = -1);
CREATE TABLE IF NOT EXISTS procedures  AS SELECT * FROM read_csv_auto(getvariable('URL')||'/'||'procedures.csv' , SAMPLE_SIZE = -1);
CREATE TABLE IF NOT EXISTS allergies   AS SELECT * FROM read_csv_auto(getvariable('URL')||'/'||'allergies.csv'  , SAMPLE_SIZE = -1);

--FILE METHOD
SET VARIABLE dpath = 'C:\github\chop-test\datasets';
/*
SET VARIABLE tab = 'patients'   ;  CREATE TABLE IF NOT EXISTS patients    AS SELECT * FROM read_csv_auto(getvariable('dpath')||'\'||getvariable('tab')||'.csv', SAMPLE_SIZE = -1);
SET VARIABLE tab = 'encounters' ;  CREATE TABLE IF NOT EXISTS encounters  AS SELECT * FROM read_csv_auto(getvariable('dpath')||'\'||getvariable('tab')||'.csv', SAMPLE_SIZE = -1);
SET VARIABLE tab = 'medications';  CREATE TABLE IF NOT EXISTS medications AS SELECT * FROM read_csv_auto(getvariable('dpath')||'\'||getvariable('tab')||'.csv', SAMPLE_SIZE = -1);
SET VARIABLE tab = 'allergies'  ;  CREATE TABLE IF NOT EXISTS allergies   AS SELECT * FROM read_csv_auto(getvariable('dpath')||'\'||getvariable('tab')||'.csv', SAMPLE_SIZE = -1);
SET VARIABLE tab = 'procedures' ;  CREATE TABLE IF NOT EXISTS procedures  AS SELECT * FROM read_csv_auto(getvariable('dpath')||'\'||getvariable('tab')||'.csv', SAMPLE_SIZE = -1);
*/

  ALTER TABLE patients    ADD CONSTRAINT patients_pk   PRIMARY KEY (Id);
  ALTER TABLE encounters  ADD CONSTRAINT encounters_pk PRIMARY KEY (Id);
--ALTER TABLE encounters  ADD CONSTRAINT encounters_patients_FK FOREIGN KEY (patient) REFERENCES patients(Id);   --not enabled in DuckDB outside create statement

  --=================================================================================================================================================================
-- TABLE COUNTS
SELECT 'patients'    AS t, 'RACE-GEN' AS binding, COUNT(DISTINCT race||gender) AS types, COUNT(*) AS recs FROM patients    UNION ALL --  11864
SELECT 'encounters'  AS t, 'SNOMED'   AS binding, COUNT(DISTINCT code)         AS types, COUNT(*) AS recs FROM encounters  UNION ALL -- 413298
SELECT 'medications' AS t, 'RxNorm'   AS binding, COUNT(DISTINCT code)         AS types, COUNT(*) AS recs FROM medications UNION ALL -- 112270
SELECT 'procedures'  AS t, 'SNOMED'   AS binding, COUNT(DISTINCT "code.x")     AS types, COUNT(*) AS recs FROM procedures  UNION ALL -- 324889
SELECT 'allergies'   AS t, 'SNOMED'   AS binding, COUNT(DISTINCT code)         AS types, COUNT(*) AS recs FROM allergies;            --   5374

-- COMPLETE OPIOID RX STATS
SELECT description, 
       COUNT(DISTINCT patient) AS patients, 
       COUNT(*)                AS prescriptions ,
       STRING_AGG(DISTINCT reasondescription, '|' order by reasondescription) AS rx_reason
  FROM medications 
  WHERE code IN (316049,429503,406022) 
 GROUP BY description, code 
 ORDER BY COUNT(*) DESC;

--=================================================================================================================================================================

CREATE OR REPLACE VIEW overdose_cohort --432 rows
AS
WITH patient_med_list AS ( --312028 encounters with active med rx.   Convert CTE to dedicated view to reuse
                           -- 64593 include opioids
                           SELECT e.patient                                                         AS patient ,
                                  e.id                                                              AS encounter ,
                                  COUNT(m.code)                                                     AS COUNT_CURRENT_MEDS,
                                  MAX(CASE WHEN m.code IN (316049,429503,406022) THEN 1 ELSE 0 END) AS CURRENT_OPIOID_IND
                             FROM encounters  e
                             JOIN medications m ON e.patient = m.patient   --inner join for performance
                                               AND e.start BETWEEN m.start AND (CASE m.stop WHEN 'NA' THEN CURRENT_DATE ELSE m.stop::DATE END)
                            GROUP BY e.patient, e.id
                            ORDER BY e.patient, e.id
                          )
  SELECT   pat.last                                                                 AS PATIENT_NAME            ,
           pat.id                                                                   AS PATIENT_ID              ,
           enc.id                                                                   AS ENCOUNTER_ID            ,
           DATE_DIFF('year',pat.BIRTHDATE, enc.START)                               AS AGE_AT_VISIT            ,
           enc.START                                                                AS HOSPITAL_ENCOUNTER_DATE ,
        -- enc.STOP                                                                 AS HOSPITAL_ENCOUNTER_STOP ,
           -----------------------------------------------------------------------------------------------------
         ROW_NUMBER() OVER (PARTITION BY pat.id ORDER BY enc.START)                 AS SEQ                     , --required for FIRST READMISSION
       LAG(enc.START) OVER (PARTITION BY pat.id ORDER BY enc.START)                 AS PRIOR_ADMISSION_DATE    ,
         -------------------------------------------------------------------------------------------------------
           CASE WHEN pat.DEATHDATE = STRFTIME(enc.STOP ::DATE,'%Y-%m-%d') THEN 1    
                WHEN pat.DEATHDATE <> 'NA'                                THEN 0    
                ELSE NULL                                                           
                END                                                                 AS DEATH_AT_VISIT_IND ,
           ------------------------------------------------------------------------------------------------     
           pat.DEATHDATE                                                            AS DEATH_DATE         ,                                                                                     
           COALESCE(pml.COUNT_CURRENT_MEDS,0)                                       AS COUNT_CURRENT_MEDS ,
           COALESCE(pml.CURRENT_OPIOID_IND,0)                                       AS CURRENT_OPIOID_IND ,
         --enc.ENCOUNTERCLASS, 
           enc.DESCRIPTION                                                          AS HOSPITAL_DESCRIPTION, 
         --enc.REASONDESCRIPTION

      FROM patients         pat 
      JOIN encounters       enc ON pat.id      = enc.patient
 LEFT JOIN patient_med_list pml ON enc.patient = pml.patient 
                               AND enc.id      = pml.encounter
     WHERE enc.reasoncode IN ('55680006')                                      --SNOMED-CT Drug overdose
       AND DATE_DIFF('year', pat.BIRTHDATE, enc.START) BETWEEN 18 and 35       --age requirement
       AND enc.START > '1999-07-15'                                            --temporal requirement
     ORDER BY pat.id, 
              enc.start  --ORDER BY for debugging, remove for performance if needed
      ;
      
 
-- 432 rows    
CREATE OR REPLACE VIEW SOLUTION
AS
SELECT 
       oc.PATIENT_ID, 
       oc.ENCOUNTER_ID,
       oc.HOSPITAL_ENCOUNTER_DATE,
       oc.AGE_AT_VISIT, 
     --oc.DEATH_DATE, 
       oc.DEATH_AT_VISIT_IND, 
       oc.COUNT_CURRENT_MEDS, 
       oc.CURRENT_OPIOID_IND,
     --          DATEDIFF('day', oc.PRIOR_ADMISSION_DATE, oc.HOSPITAL_ENCOUNTER_DATE)                                                            AS DAYS_FROM_PRIOR,
       CASE WHEN DATEDIFF('day', oc.PRIOR_ADMISSION_DATE, oc.HOSPITAL_ENCOUNTER_DATE) <= 90 THEN 1 ELSE 0 END                                    AS READMISSION_90_DAY_IND,
       CASE WHEN DATEDIFF('day', oc.PRIOR_ADMISSION_DATE, oc.HOSPITAL_ENCOUNTER_DATE) <= 30 THEN 1 ELSE 0 END                                    AS READMISSION_30_DAY_IND,
       CASE WHEN DATEDIFF('day', oc.PRIOR_ADMISSION_DATE, oc.HOSPITAL_ENCOUNTER_DATE) <= 90 THEN oc.PRIOR_ADMISSION_DATE::VARCHAR ELSE 'N/A' END AS FIRST_READMISSION_DATE
     FROM overdose_cohort oc;
     
     
COPY (SELECT * FROM SOLUTION) TO 'C:\github\chop-test\solution\MICHAEL_DIGIANTOMASSO.csv' (HEADER, DELIMITER ',');
 
 

--QA CHECK
 
    SELECT COUNT(DISTINCT PATIENT_ID)   AS patients,  
           COUNT(*)                     AS drug_od_hosptial_ER_encounters,
           SUM(DEATH_AT_VISIT_IND)      AS deaths,                          -- SELECT * FROM overdose_cohort WHERE DEATH_AT_VISIT_IND = 1
           SUM(CURRENT_OPIOID_IND)      AS encouters_with_opioid_rx ,       -- SELECT * FROM overdose_cohort WHERE CURRENT_OPIOID_IND = 1
           SUM(READMISSION_90_DAY_IND)  AS READM_90,
           SUM(READMISSION_30_DAY_IND)  AS READM_30
      FROM SOLUTION;
    