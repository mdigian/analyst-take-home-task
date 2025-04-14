/*
Prereqs
Fork https://github.com/chop-analytics/analyst-take-home-task
clone forked repo to local
tools
   sourcetree
   DBeaver
*/


SET VARIABLE dpath = 'C:\github\chop-task\datasets';

--EXECUTE IMMEDIATE doesnt work for CREATE/DROP TABLE.  For more dynamic automation, use python with duckdb lib

--All objects created in memory.main db schema

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




