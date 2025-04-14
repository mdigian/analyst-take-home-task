/* Assemble project cohort 

identify patients seen for drug OD
Determine active opioid at encounter start
SHOW drug OD readmissions count

assemble cohort by identifying encounters that meet

patient’s visit is encounter for drug OD
hospital encounter > 1999-07-15
patient age (at time of encounter) BETWEEN 18 and 35 ( >=18 AND < 36)

*/

--==================================================================================================
 SELECT Id, BIRTHDATE, DEATHDATE, LAST, RACE, ETHNICITY, GENDER
   FROM patients ORDER BY BIRTHDATE  ;


SELECT   pat.last                                     AS patient_name,
         DATE_DIFF('year',pat.BIRTHDATE, START)       AS age_at_encounter,
       --STRFTIME(pat.BIRTHDATE,'%Y-%m-%d')           AS BIRTHDATE,
       --Id, 
         STRFTIME(START::DATE,'%Y-%m-%d')             AS encounter_START, 
         STRFTIME(STOP ::DATE,'%Y-%m-%d')             AS encounter_STOP, 
         pat.DEATHDATE,
       --PROVIDER, 
         ENCOUNTERCLASS, 
         DESCRIPTION, 
         REASONDESCRIPTION
    FROM patients   pat 
    JOIN encounters enc ON pat.id = enc.patient
   WHERE reasoncode IN ('55680006') --SNOMED-CT overdose code
     AND DATE_DIFF('year',pat.BIRTHDATE, START)  BETWEEN 18 and 35
     AND START > '1999-07-15'
   ORDER BY pat.id, 
            enc.id;
--==================================================================================================


    SELECT 
           ENCOUNTERCLASS                                          AS class, 
           COUNT(DISTINCT CODE)                                    AS codes, 
           COUNT(DISTINCT REASONCODE)                              AS reason_codes ,
           COUNT(DISTINCT patient)                                 AS patients,
           COUNT(*)                                                AS occurrences ,
           STRING_AGG(DISTINCT description||' - '||reasondescription, '\n' 
                      ORDER BY description||' - '||reasondescription)       AS description
      FROM encounters 
     WHERE reasoncode = '55680006'
  GROUP BY ENCOUNTERCLASS 
  ORDER BY occurrences DESC;
    
--==================================================================================================    
    
SELECT DATE, 
       PATIENT, 
       ENCOUNTER, 
       CODE, 
       DESCRIPTION, 
       COST, 
       REASONCODE, 
       REASONDESCRIPTION
FROM procedures;

--==================================================================================================    

SELECT PATIENT, 
       ENCOUNTER, 
       CODE, 
       DESCRIPTION, 
       START, 
       STOP, 
       DISPENSES, 
       REASONCODE, 
       REASONDESCRIPTION,
       COST,
       TOTALCOST
   FROM medications
  --WHERE LOWER(description) LIKE '%opiod%';
  ORDER BY patient, encounter