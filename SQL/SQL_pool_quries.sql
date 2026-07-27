-- ============================================================
-- IMPORTANT: Dedicated SQL pools (CREATE EXTERNAL TABLE ... FILE_FORMAT =
-- ParquetFileFormat) do NOT understand the Delta Lake transaction log
-- (_delta_log/). Pointing a plain-Parquet external table at a path that
-- Databricks wrote as Delta can return duplicate rows, stale/orphaned
-- files, or fail outright, because Delta keeps historical Parquet files
-- around that a naive Parquet reader has no way to filter out.
--
-- Fix: use a SERVERLESS SQL pool and OPENROWSET(..., FORMAT = 'DELTA'),
-- which natively reads the Delta transaction log and returns only the
-- current, correct set of rows. This script targets a serverless pool.
-- ============================================================

CREATE MASTER KEY ENCRYPTION BY PASSWORD = '<<Password>>';

-- Credential (Managed Identity avoids embedding storage keys)
CREATE DATABASE SCOPED CREDENTIAL storage_credential
WITH IDENTITY = 'Managed Identity';

-- Data source pointing at the gold container root
CREATE EXTERNAL DATA SOURCE gold_data_source
WITH (
    LOCATION = 'abfss://<<container>>@<<Storageaccount_name>>.dfs.core.windows.net/',
    CREDENTIAL = storage_credential
);

-- ---------------------------------------------------------------
-- Delta-aware views (replace the old CREATE EXTERNAL TABLE ... Parquet
-- definitions). Downstream views/Power BI can query these exactly like
-- tables -- column names and shapes are unchanged, so nothing downstream
-- needs to be rewritten.
-- ---------------------------------------------------------------

CREATE VIEW dbo.dim_patient AS
SELECT
    patient_id,
    gender,
    age,
    effective_from,
    surrogate_key,
    effective_to,
    is_current
FROM OPENROWSET(
    BULK 'dim_patient/',
    DATA_SOURCE = 'gold_data_source',
    FORMAT = 'DELTA'
) AS result;
GO

CREATE VIEW dbo.dim_department AS
SELECT
    surrogate_key,
    department,
    hospital_id
FROM OPENROWSET(
    BULK 'dim_department/',
    DATA_SOURCE = 'gold_data_source',
    FORMAT = 'DELTA'
) AS result;
GO

CREATE VIEW dbo.fact_patient_flow AS
SELECT
    fact_id,
    patient_sk,
    department_sk,
    admission_time,
    discharge_time,
    admission_date,
    length_of_stay_hours,
    is_currently_admitted,
    bed_id,
    event_ingestion_time
FROM OPENROWSET(
    BULK 'fact_patient_flow/',
    DATA_SOURCE = 'gold_data_source',
    FORMAT = 'DELTA'
) AS result;
GO

SELECT TOP 100 * FROM dbo.fact_patient_flow;

-- ---------------------------------------------------------------
-- ALTERNATIVE, if a dedicated pool is a hard requirement:
-- Materialize a clean Parquet snapshot from serverless with CETAS, then
-- point dedicated-pool CREATE EXTERNAL TABLE at that snapshot instead of
-- the raw Delta path. Re-run the CETAS on each gold refresh.
--
-- CREATE EXTERNAL TABLE dbo.fact_patient_flow_snapshot
-- WITH (LOCATION = 'snapshots/fact_patient_flow/', DATA_SOURCE = gold_data_source,
--       FILE_FORMAT = ParquetFileFormat)
-- AS SELECT * FROM dbo.fact_patient_flow;
-- ---------------------------------------------------------------
