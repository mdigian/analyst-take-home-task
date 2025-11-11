-- ==========================================================
-- Analyst Take-Home Task | CHOP → Oliver James → Vanguard Charitable
-- Author: Mark Holahan
-- Date: Monday, 10/27/25
-- ----------------------------------------------------------
-- Purpose:
--   Build an overdose-encounter cohort and compute derived
--   patient-level metrics per CHOP data-exercise instructions.
--
-- Description:
--   • Implements fully parameterized, portable ETL in DuckDB
--   • Cleans “NA” values, enforces datatypes via TRY_CAST
--   • Establishes cohort of patients aged 18–35 with encounters
--     coded as “Drug overdose” (SNOMED 55680006)
--   • Derives flags and metrics:
--       – death at visit
--       – active medication count at encounter start
--       – current opioid indicator
--       – 30 / 90 day readmission flags
--       – first readmission date (≤ 90 days)
--   • Produces two export files:
--       1) patient_encounters.csv – row-level cohort
--       2) metrics_summary.csv – aggregated summary
--
-- Environment:
--   • DuckDB (v1.0 or later) executed via DBeaver or CLI
--   • All paths relative to base_dir variable below
--
-- Notes:
--   • Variable drug_overdose_code holds the SNOMED constant
--   • Descriptive aliases used in lieu of single-letter ones
--   • ETL is idempotent and re-runnable end-to-end
-- ==========================================================

-- ==========================================================
-- 1. VARIABLES
-- ==========================================================

SET variable base_dir           = 'C:\github\chop-test';
SET variable patient_encounters = concat(getvariable('base_dir'), '\solution\patient_encounters.csv');
SET variable oliver_metrics     = concat(getvariable('base_dir'), '\solution\metrics_summary.csv');
SET variable drug_overdose_code = '55680006';  -- SNOMED: Drug overdose
-- ==========================================================
-- 2. INGEST RAW DATASETS
-- ==========================================================
-- 2a. PATIENTS
CREATE OR REPLACE TABLE patients AS
SELECT
    TRY_CAST(NULLIF(Id, 'NA')   AS UUID) AS Id,
    TRY_CAST(BirthDate AS DATE) AS BirthDate,
    TRY_CAST(NULLIF(DeathDate, 'NA') AS DATE) AS DeathDate,
    NULLIF(SSN, 'NA')     AS SSN,
    NULLIF(First, 'NA')   AS First,
    NULLIF(Last, 'NA')    AS Last,
    NULLIF(Gender, 'NA')  AS Gender,
    NULLIF(State, 'NA')   AS State,
    NULLIF(City, 'NA')    AS City
FROM read_csv_auto(
    concat(getvariable('base_dir'), '\datasets\patients.csv'),
    SAMPLE_SIZE=-1, AUTO_DETECT=TRUE, HEADER=TRUE
);

-- 2b. ENCOUNTERS
CREATE OR REPLACE TABLE encounters AS
SELECT
    TRY_CAST(NULLIF(Id, 'NA') AS UUID) AS Id,
    TRY_CAST(NULLIF(Start, 'NA') AS DATE) AS Start,
    TRY_CAST(NULLIF(Stop, 'NA') AS DATE) AS Stop,
    TRY_CAST(NULLIF(Patient, 'NA') AS UUID) AS Patient,
    TRY_CAST(NULLIF(Provider, 'NA') AS UUID) AS Provider,
    NULLIF(EncounterClass, 'NA') AS EncounterClass,
    NULLIF(Code, 'NA') AS Code,
    NULLIF(Description, 'NA') AS Description,
    TRY_CAST(NULLIF(Cost, 'NA') AS DOUBLE) AS Cost,
    NULLIF(Reasoncode, 'NA') AS Reasoncode,
    NULLIF(ReasonDescription, 'NA') AS ReasonDescription
FROM read_csv_auto(
    concat(getvariable('base_dir'), '\datasets\encounters.csv'),
    SAMPLE_SIZE=-1, AUTO_DETECT=TRUE, HEADER=TRUE,
    TYPES={'Start': 'VARCHAR', 'Stop': 'VARCHAR', 'Cost': 'VARCHAR'}
);

-- 2c. MEDICATIONS
CREATE OR REPLACE TABLE medications AS
SELECT
    TRY_CAST(NULLIF(Start, 'NA') AS DATE) AS Start,
    TRY_CAST(NULLIF(Stop, 'NA') AS DATE) AS Stop,
    TRY_CAST(NULLIF(Patient, 'NA') AS UUID) AS Patient,
    TRY_CAST(NULLIF(Encounter, 'NA') AS UUID) AS Encounter,
    NULLIF(Code, 'NA') AS Code,
    NULLIF(Description, 'NA') AS Description,
    TRY_CAST(NULLIF(Cost, 'NA') AS DOUBLE) AS Cost,
    NULLIF(Reasoncode, 'NA') AS Reasoncode,
    NULLIF(ReasonDescription, 'NA') AS ReasonDescription
FROM read_csv_auto(
    concat(getvariable('base_dir'), '\datasets\medications.csv'),
    SAMPLE_SIZE=-1, AUTO_DETECT=TRUE, HEADER=TRUE,
    TYPES={'Start': 'VARCHAR', 'Stop': 'VARCHAR', 'Cost': 'VARCHAR', 'Code': 'VARCHAR'}
);


-- ==========================================================
-- 3. BUILD PROJECT COHORT
-- ==========================================================
CREATE OR REPLACE TABLE cohort AS
SELECT
    enc.Id            AS encounter_id,
    enc.Start         AS hospital_encounter_date,
    pat.Id            AS patient_id,
    enc.Reasoncode,
    pat.Birthdate,
    pat.Deathdate,
    DATE_DIFF('year', pat.Birthdate, enc.Start) AS age_at_visit
FROM encounters enc
JOIN patients pat ON enc.Patient = pat.Id
WHERE enc.Reasoncode = getvariable('drug_overdose_code')
  AND enc.Start > '1999-07-15'
  AND DATE_DIFF('year', pat.Birthdate, enc.Start) BETWEEN 18 AND 35;


-- ==========================================================
-- 4. DERIVE ENRICHED METRICS
-- ==========================================================

CREATE OR REPLACE TABLE enriched AS
WITH base_tbl AS (
    SELECT
        cohort_tbl.*,
        CASE
            WHEN (patients_tbl.DeathDate IS NOT NULL 
                  AND DATE_DIFF('day', encounters_tbl.Start, patients_tbl.DeathDate) BETWEEN 0 AND 1)
            THEN 1 ELSE 0 
        END AS death_at_visit_ind
    FROM cohort cohort_tbl
    JOIN encounters encounters_tbl ON encounters_tbl.Id = cohort_tbl.encounter_id
    JOIN patients patients_tbl ON patients_tbl.Id = cohort_tbl.patient_id
),
med_stats AS (
    SELECT
        meds.Patient AS patient_id,
        COUNT(*) FILTER (WHERE (meds.Stop IS NULL OR meds.Stop >= meds.Start)) AS count_current_meds,
        MAX(CASE
                WHEN UPPER(meds.Description) IN (
                    'HYDROMORPHONE 325MG',
                    'FENTANYL 100 MCG',
                    'OXYCODONE-ACETAMINOPHEN 100 MI'
                ) THEN 1 ELSE 0
            END) AS current_opioid_ind
    FROM medications meds
    GROUP BY meds.Patient
)
SELECT
    base_tbl.*,
    med_stats.count_current_meds,
    med_stats.current_opioid_ind,

    CASE WHEN EXISTS (
        SELECT 1
        FROM encounters next_enc
        WHERE next_enc.Patient = base_tbl.patient_id
          AND next_enc.Reasoncode = getvariable('drug_overdose_code')
          AND next_enc.Start > base_tbl.hospital_encounter_date
          AND DATE_DIFF('day', base_tbl.hospital_encounter_date, next_enc.Start) <= 90
    ) THEN 1 ELSE 0 END AS readmission_90_day_ind,

    CASE WHEN EXISTS (
        SELECT 1
        FROM encounters next_enc
        WHERE next_enc.Patient = base_tbl.patient_id
          AND next_enc.Reasoncode = getvariable('drug_overdose_code')
          AND next_enc.Start > base_tbl.hospital_encounter_date
          AND DATE_DIFF('day', base_tbl.hospital_encounter_date, next_enc.Start) <= 30
    ) THEN 1 ELSE 0 END AS readmission_30_day_ind,

    (
        SELECT MIN(next_enc.Start)
        FROM encounters next_enc
        WHERE next_enc.Patient = base_tbl.patient_id
          AND next_enc.Reasoncode = getvariable('drug_overdose_code')
          AND next_enc.Start > base_tbl.hospital_encounter_date
          AND DATE_DIFF('day', base_tbl.hospital_encounter_date, next_enc.Start) <= 90
    ) AS first_readmission_date

FROM base_tbl
LEFT JOIN med_stats ON base_tbl.patient_id = med_stats.patient_id;


-- ==========================================================
-- 5. EXPORT RESULTS
-- ==========================================================

COPY (
    SELECT
        patient_id,
        encounter_id,
        hospital_encounter_date,
        age_at_visit,
        death_at_visit_ind,
        count_current_meds,
        current_opioid_ind,
        readmission_90_day_ind,
        readmission_30_day_ind,
        first_readmission_date
    FROM enriched
    ORDER BY patient_id, hospital_encounter_date
)
TO (getvariable('patient_encounters'))
WITH (HEADER, DELIMITER ',');

COPY (
    SELECT
        COUNT(*) AS total_patient_encounters,
        COUNT(DISTINCT patient_id) AS distinct_patients,
        SUM(current_opioid_ind) AS active_opioid_rx_encounters,
        SUM(death_at_visit_ind) AS deaths_during_overdose,
        SUM(readmission_90_day_ind) AS readmissions_90d,
        SUM(readmission_30_day_ind) AS readmissions_30d
    FROM enriched
)
TO (getvariable('oliver_metrics'))
WITH (HEADER, DELIMITER ',');

-- ==========================================================
-- END OF SCRIPT
-- ==========================================================
