# 🚑 MediFlowRT: Real-Time Healthcare Analytics Platform on Azure

> **An end-to-end Azure Data Engineering project demonstrating real-time
> healthcare event ingestion, Medallion Architecture (Bronze → Silver →
> Gold), dimensional modeling (SCD Type 2), Synapse Serverless SQL, and
> Power BI analytics.**

![Azure](https://img.shields.io/badge/Azure-Cloud-blue?logo=microsoft-azure&style=flat-square)
![Databricks](https://img.shields.io/badge/Databricks-PySpark-red?logo=databricks&style=flat-square)
![PySpark](https://img.shields.io/badge/PySpark-Structured%20Streaming-orange?logo=apache-spark&style=flat-square)
![Azure
Synapse](https://img.shields.io/badge/Azure-Synapse-blue?logo=microsoft-azure&style=flat-square)
![Power
BI](https://img.shields.io/badge/Power%20BI-Dashboard-yellow?logo=power-bi&style=flat-square)
![Python](https://img.shields.io/badge/Python-3.9+-blue?logo=python&style=flat-square)

------------------------------------------------------------------------

## 📑 Table of Contents

-   [Project Overview](#project-overview)
-   [Architecture](#architecture)
-   [Objectives](#objectives)
-   [Project Structure](#project-structure)
-   [Tools & Technologies](#tools--technologies)
-   [Data Architecture](#data-architecture)
-   [Star Schema](#star-schema)
-   [Security & Secrets Management](#security--secrets-management)
-   [Data Quality Approach](#data-quality-approach)
-   [Implementation](#implementation)
-   [Data Analytics](#data-analytics)
-   [Known Limitations](#known-limitations)
-   [Key Outcomes](#key-outcomes)
-   [License](#license)

------------------------------------------------------------------------

## Project Overview

This project implements a production-inspired healthcare analytics
pipeline on Microsoft Azure. Streaming patient events are ingested
through Azure Event Hub, processed with Databricks Structured Streaming,
transformed using a Medallion Architecture, exposed through Synapse
Serverless SQL, and visualized in Power BI.

### Highlights

-   Real-time ingestion using Azure Event Hub
-   Databricks Structured Streaming ETL
-   Bronze → Silver → Gold Medallion Architecture
-   SCD Type 2 dimensional modeling
-   Delta Lake storage
-   Synapse Serverless SQL
-   Interactive Power BI dashboard
-   Secret Scope credential management

## Architecture

> Replace this section with your architecture diagram.

------------------------------------------------------------------------

## Objectives

-   Stream patient events from Event Hub.
-   Validate and clean incoming records.
-   Build dimensional Gold tables.
-   Query Delta Lake through Synapse Serverless SQL.
-   Deliver interactive Power BI reporting.

------------------------------------------------------------------------

## Project Structure

``` text
MediFlowRT/
│
├── databricks_notebook/
│   ├── 01_bronze_rawdata.py
│   ├── 02_silver_cleandata.py
│   └── 03_gold_transform.py
│
├── simulator/
│   └── patient_flow_generator.py
│
├── SQL/
│   ├── SQL_pool_quries.sql
│   └── SQL_views_DDL.sql
│
├── client_requirements/
│   └── client_requirements_de.pdf
│
├── Hospital_Dashboard.pbix
├── README.md
└── LICENSE
```

------------------------------------------------------------------------

## Tools & Technologies

  Category          Technology
  ----------------- ------------------------------------------
  Streaming         Azure Event Hub
  Processing        Azure Databricks, PySpark
  Storage           Azure Data Lake Storage Gen2, Delta Lake
  Analytics         Synapse Serverless SQL
  Visualization     Power BI
  Language          Python
  Version Control   Git

------------------------------------------------------------------------

## Data Architecture

-   **Bronze:** Raw streaming events stored as Delta.
-   **Silver:** Cleansed and validated data with quality flags.
-   **Gold:** SCD Type 2 dimensions and fact table optimized for BI.

## Star Schema

**Fact**

-   `fact_patient_flow`

**Dimensions**

-   `dim_patient`
-   `dim_department`

------------------------------------------------------------------------

## Security & Secrets Management

-   Databricks Secret Scopes
-   Environment variables for simulator credentials
-   Managed Identity for Synapse access
-   No secrets stored in source code

------------------------------------------------------------------------

## Data Quality Approach

-   Validate age and timestamps
-   Flag invalid records
-   Preserve raw data
-   Exclude invalid records from Gold while retaining them in Silver for
    auditing

------------------------------------------------------------------------

## Implementation

### 1. Event Hub

Configure Event Hub and Kafka-compatible endpoint.

### 2. Data Simulation

[Producer Code](simulator/patient_flow_generator.py)

### 3. Storage

Create Bronze, Silver and Gold Delta Lake containers.

### 4. Databricks

-   [Notebook 1](databricks_notebook/01_bronze_rawdata.py)
-   [Notebook 2](databricks_notebook/02_silver_cleandata.py)
-   [Notebook 3](databricks_notebook/03_gold_transform.py)

### 5. Synapse Serverless SQL

-   [SQL_pool_quries.sql](SQL/SQL_pool_quries.sql)
-   [SQL_views_DDL.sql](SQL/SQL_views_DDL.sql)

### 6. Power BI

Connect Synapse Serverless SQL and build KPI dashboards.

------------------------------------------------------------------------

## Data Analytics

Dashboard KPIs include:

-   Department workload
-   Patient flow trends
-   Length of stay
-   Current admissions
-   Gender analysis
-   Interactive slicers

------------------------------------------------------------------------

## Known Limitations

-   Synthetic dataset only
-   Occupancy metric is a proxy
-   Gold layer refresh is scheduled batch
-   Designed as a portfolio demonstration

------------------------------------------------------------------------

## Key Outcomes

-   Production-style Azure architecture
-   Real-time streaming pipeline
-   Modern dimensional modeling
-   Business-ready analytics
-   Strong Azure Data Engineering portfolio project

------------------------------------------------------------------------

## License

This project is licensed under the MIT License. See the
[LICENSE](LICENSE) file for details.
