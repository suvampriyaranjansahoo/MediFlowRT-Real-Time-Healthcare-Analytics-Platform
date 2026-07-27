--KPI's
--1.Beds occupied total
-- NOTE: This measures the % of RECORDED VISITS currently admitted, not true
-- physical bed occupancy (admitted patients / total physical beds). There is
-- no bed-capacity dimension in this model, so this is a proxy metric, not a
-- literal occupancy rate. If real bed capacity per department/hospital is
-- available, add a dim_bed_capacity table and recompute this as
-- (currently admitted / total_beds) instead.
CREATE VIEW vw_bed_occupancy AS
SELECT 
    p.gender,
    COUNT(CASE WHEN f.is_currently_admitted = 1 THEN f.bed_id END) * 1.0 / COUNT(f.bed_id) * 100 AS bed_occupancy_percent
FROM dbo.fact_patient_flow f
JOIN dbo.dim_patient p ON f.patient_sk = p.surrogate_key
GROUP BY p.gender;
GO

--2.Total bed turnover
CREATE VIEW vw_bed_turnover_rate AS
SELECT 
    p.gender,
    COUNT(DISTINCT f.fact_id) * 1.0 / COUNT(DISTINCT f.bed_id) AS bed_turnover_rate
FROM dbo.fact_patient_flow f
JOIN dbo.dim_patient p ON f.patient_sk = p.surrogate_key
GROUP BY p.gender;
GO

--3.Total patients
CREATE VIEW vw_patient_demographics AS
SELECT 
    p.gender,
    COUNT(CASE WHEN f.is_currently_admitted = 1 THEN f.fact_id END) AS total_patients
FROM dbo.fact_patient_flow f
JOIN dbo.dim_patient p ON f.patient_sk = p.surrogate_key
GROUP BY p.gender;
GO

--4.Avg treatment duration
CREATE VIEW vw_avg_treatment_duration AS
SELECT 
    d.department,
    p.gender,
    AVG(f.length_of_stay_hours) AS avg_treatment_duration
FROM dbo.fact_patient_flow f
JOIN dbo.dim_patient p ON f.patient_sk = p.surrogate_key
JOIN dbo.dim_department d ON f.department_sk = d.surrogate_key
GROUP BY d.department, p.gender;
GO

--Chart's
--1.Total patients count over time
CREATE VIEW vw_patient_volume_trend AS
SELECT 
    f.admission_date,
    p.gender,
    COUNT(DISTINCT f.fact_id) AS patient_count
FROM dbo.fact_patient_flow f
JOIN dbo.dim_patient p ON f.patient_sk = p.surrogate_key
GROUP BY f.admission_date, p.gender;
GO

--2. Total patients over department
CREATE VIEW vw_department_inflow AS
SELECT 
    d.department,
    p.gender,
    COUNT(CASE WHEN f.is_currently_admitted = 1 THEN f.fact_id END) AS patient_count
FROM dbo.fact_patient_flow f
JOIN dbo.dim_patient p ON f.patient_sk = p.surrogate_key
JOIN dbo.dim_department d ON f.department_sk = d.surrogate_key
GROUP BY d.department, p.gender;
GO

--3.Total overstay patients count (fixed threshold view, kept for
--backward compatibility with the existing Power BI report)
CREATE VIEW vw_overstay_patients AS
SELECT 
    d.department,
    p.gender,
    COUNT(f.fact_id) AS overstay_count
FROM dbo.fact_patient_flow f
JOIN dbo.dim_patient p ON f.patient_sk = p.surrogate_key
JOIN dbo.dim_department d ON f.department_sk = d.surrogate_key
WHERE f.length_of_stay_hours > 50
GROUP BY d.department, p.gender;
GO

--3b. Parameterized version: lets a report or ad-hoc analyst pass any
--overstay threshold instead of the hardcoded 50 hours above.
--Usage: SELECT * FROM dbo.fn_overstay_patients(72);
CREATE FUNCTION dbo.fn_overstay_patients (@threshold_hours FLOAT)
RETURNS TABLE
AS
RETURN
(
    SELECT 
        d.department,
        p.gender,
        COUNT(f.fact_id) AS overstay_count
    FROM dbo.fact_patient_flow f
    JOIN dbo.dim_patient p ON f.patient_sk = p.surrogate_key
    JOIN dbo.dim_department d ON f.department_sk = d.surrogate_key
    WHERE f.length_of_stay_hours > @threshold_hours
    GROUP BY d.department, p.gender
);
GO
