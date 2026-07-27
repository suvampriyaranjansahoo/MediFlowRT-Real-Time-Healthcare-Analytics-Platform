# Real-Time Patient Flow Analytics on Azure

![Azure](https://img.shields.io/badge/Azure-Cloud-blue?logo=microsoft-azure&style=flat-square)
![PySpark](https://img.shields.io/badge/PySpark-Big%20Data-orange?logo=apache-spark&style=flat-square)
![Azure Data Factory](https://img.shields.io/badge/Azure-Data%20Factory-blue?logo=microsoft-azure&style=flat-square)
![Azure Synapse](https://img.shields.io/badge/Azure-Synapse%20Analytics-blue?logo=microsoft-azure&style=flat-square)
![Python](https://img.shields.io/badge/Python-3.9+-yellow?logo=python&style=flat-square)
![Databricks](https://img.shields.io/badge/Databricks-PySpark-red?logo=databricks&style=flat-square)
![PowerBI](https://img.shields.io/badge/Power%20BI-Dashboard-orange?logo=power-bi&style=flat-square)
![Git](https://img.shields.io/badge/Git-CI%2FCD-green?logo=git&style=flat-square)

---

## 📑 Table of Contents
- [📌 Project Overview](#-project-overview)
- [🎯 Objectives](#-objectives)
- [📂 Project Structure](#-project-structure)
- [🛠️ Tools & Technologies](#️-tools--technologies)
- [📐 Data Architecture](#-data-architecture)
- [⭐ Star Schema Design](#-star-schema-design)
- [🔐 Security & Secrets Management](#-security--secrets-management)
- [🧹 Data Quality Approach](#-data-quality-approach)
- [⚙️ Step-by-Step Implementation](#️-step-by-step-implementation)
  - [1. Event Hub Setup](#1-event-hub-setup)
  - [2. Data Simulation](#2-data-simulation)
  - [3. Storage Setup](#3-storage-setup)
  - [4. Databricks Processing](#4-databricks-processing)
  - [5. Synapse Serverless SQL](#5-synapse-serverless-sql)
  - [6. Version Control](#6-version-control)
- [📊 Data Analytics](#-data-analytics)
- [⚠️ Known Limitations](#️-known-limitations)
- [✅ Key Outcomes](#-key-outcomes)
- [📜 License](#-license)

---

## 📌 Project Overview
This project demonstrates an **end-to-end data engineering pipeline** for healthcare, designed to analyze **patient flow across hospital departments** using Azure cloud services.
The pipeline ingests streaming patient events, cleans and models them in **Databricks (PySpark)**, curates a dimensional model with slowly changing dimensions, and exposes it through **Synapse serverless SQL** for **Power BI** analytics.

**Part 1 – Data Engineering:** Streaming ingestion + cleansing, batch-refreshed dimensional modeling.
**Part 2 – Analytics:** Synapse-to-Power BI star schema and an interactive hospital KPI dashboard.

## Pipeline

<img width="4719" height="2432" alt="Architecture" src="https://github.com/user-attachments/assets/cb1a1775-ab64-45d7-b45b-50ba97660e1d" />

---

## 🎯 Objectives
- Ingest patient events in **near real-time** via Azure Event Hub (Kafka-compatible endpoint).
- Process and cleanse data using **Databricks Structured Streaming** (Bronze → Silver), flagging bad records instead of silently rewriting them.
- Refresh a **curated Gold layer** (SCD Type 2 dimensions + fact table) on a scheduled cadence — batch by design, not an oversight (see [Data Architecture](#-data-architecture)).
- Expose Gold as a **star schema** through Synapse serverless SQL, queried directly by Power BI.
- Manage all credentials through a **Databricks secret scope**, never hardcoded in notebooks.

---

## 📂 Project Structure
```plaintext
real-time-patient-flow-azure/
│
├── databricks-notebooks/          # Transformation notebooks
│   ├── 01_bronze_rawdata.py       # Event Hub -> Bronze (streaming)
│   ├── 02_silver_cleandata.py     # Bronze -> Silver (streaming, data-quality flagging)
│   └── 03_gold_transform.py       # Silver -> Gold (batch, SCD2 dimensions + fact)
├── simulator/                     # Data simulation scripts
│   └── patient_flow_generator.py
├── sqlpool-quries/                # SQL scripts for Synapse
│   ├── SQL_views_DDL.sql          # KPI views consumed by Power BI
│   └── SQL_pool_quries.sql        # Serverless Delta-aware views over Gold
├── git_commands/                  # Git command reference
└── README.md                      # Project documentation
```

---

## 🛠️ Tools & Technologies
- **Azure Event Hub** – Real-time data ingestion (Kafka-compatible protocol)
- **Azure Databricks** – PySpark Structured Streaming ETL
- **Azure Data Lake Storage (Delta Lake)** – Bronze / Silver / Gold storage
- **Azure Synapse Serverless SQL** – Delta-aware analytics layer (`OPENROWSET ... FORMAT = 'DELTA'`)
- **Power BI** – Dashboarding
- **Databricks Secret Scopes / Key Vault** – Credential management
- **Python 3.9+** – Core programming
- **Git** – Version control

---

## 📐 Data Architecture
The pipeline follows a **multi-layered Medallion architecture**:

- **Bronze Layer** (streaming): Raw JSON events from Event Hub, written as-is to Delta, with Kafka offset/partition/timestamp preserved for lineage and replay.
- **Silver Layer** (streaming): Parsed and typed data. Invalid values (implausible ages, future admission timestamps) are **nulled and flagged** via a `data_quality_flag` / `is_valid_record` column rather than being silently replaced — see [Data Quality Approach](#-data-quality-approach).
- **Gold Layer** (scheduled batch): Deduplicated, dimensionally modeled data — SCD Type 2 `dim_patient`, slowly changing `dim_department`, and a partitioned `fact_patient_flow` table, ready for BI consumption.

**Why Gold is batch, not streaming:** SCD2 change detection and dimensional joins are far easier to get correct and auditable as a scheduled batch job (with explicit MERGE-style update/insert logic) than as a continuously-updating stream. This is a deliberate, common hybrid pattern — dashboard freshness is bounded by the Gold refresh schedule, not by ingestion latency, and that trade-off is intentional rather than a limitation of the streaming layers above it.

---

## ⭐ Star Schema Design
The **Gold layer** follows a **star schema**:
- **Fact Table**: `fact_patient_flow` (admission/discharge times, length of stay, current-admission flag, bed ID) — partitioned by `admission_date`.
- **Dimension Tables**:
  - `dim_patient` – Patient demographics, modeled as **SCD Type 2** (tracks history via `surrogate_key`, `effective_from`, `effective_to`, `is_current`) so changes in gender/age over time are preserved rather than overwritten.
  - `dim_department` – Department and hospital details.

Synapse exposes these as Delta-aware views (`OPENROWSET ... FORMAT = 'DELTA'`) rather than plain-Parquet external tables, since Delta's transaction log needs to be read natively to avoid returning stale or duplicate files.

---

## 🔐 Security & Secrets Management
- Storage account keys and the Event Hub connection string are **never hardcoded**. Databricks notebooks pull them from a secret scope:
  ```
  databricks secrets create-scope --scope patient-flow-kv
  databricks secrets put --scope patient-flow-kv --key eventhub-conn-str
  databricks secrets put --scope patient-flow-kv --key storage-account-key
  ```
  (or link the scope directly to an Azure Key Vault instance).
- The simulator reads its Event Hub connection string from an environment variable (`EVENTHUB_CONNECTION_STRING`) rather than source code.
- Synapse uses a **Managed Identity**-backed database scoped credential rather than an embedded storage key.

---

## 🧹 Data Quality Approach
Rather than silently repairing bad data (which destroys the signal that a record was ever invalid and can fabricate misleading values), the Silver layer:
1. Detects invalid `age` (outside 1–100) and invalid `admission_time` (missing or set in the future).
2. **Nulls only the specific invalid field**, preserving the rest of the record.
3. Records *why* via a `data_quality_flag` column and an `is_valid_record` boolean.
4. The Gold layer filters to `is_valid_record = true` before building dimensions/facts, so bad data is quarantined in Silver rather than flowing into the curated BI layer — while still being inspectable there for monitoring/debugging.

---

## ⚙️ Step-by-Step Implementation

### **1. Event Hub Setup**
- Created **Event Hub namespace** and **patient-flow hub**.
- Configured **consumer groups** for Databricks streaming (accessed via the Kafka-compatible endpoint).

---

### **2. Data Simulation**
- `patient_flow_generator.py` streams synthetic patient events (departments, admission/discharge times, dirty-data injection) to Event Hub.
- ~15% of generated events reuse a recently-seen `patient_id`, simulating re-admissions so the SCD2 logic downstream has real change data to detect.
- [Producer Code](simulator/patient_flow_generator.py)

---

### **3. Storage Setup**
- Configured **Azure Data Lake Storage (ADLS Gen2)**.
- Created containers for **bronze**, **silver**, and **gold** layers.

---

### **4. Databricks Processing**
- [**Notebook 1**](databricks-notebooks/01_bronze_rawdata.py): Streams Event Hub data into Bronze, preserving Kafka metadata.
- [**Notebook 2**](databricks-notebooks/02_silver_cleandata.py): Parses, types, and flags invalid records.
- [**Notebook 3**](databricks-notebooks/03_gold_transform.py): Builds SCD2 dimensions and the fact table.

---

### **5. Synapse Serverless SQL**
- Created a **serverless SQL pool** with a Managed Identity credential.
- Delta-aware views defined in [`SQL_pool_quries.sql`](sqlpool-quries/SQL_pool_quries.sql) expose Gold tables to downstream consumers.
- KPI/reporting views defined in [`SQL_views_DDL.sql`](sqlpool-quries/SQL_views_DDL.sql), including a parameterized `fn_overstay_patients(@threshold_hours)` function for flexible overstay analysis.

---

### **6. Version Control**
- Version control with **Git**:
  - [Commands reference](git_commands/git_bash)

---

## 📊 Data Analytics

Once the pipeline was established and the star schema implemented, the next step was building an **interactive dashboard in Power BI**.

### **🔗 Synapse → Power BI Connection**
- Connected **Synapse serverless SQL** to Power BI via a direct SQL connection.
- Imported `fact_patient_flow` and dimension views.
- Established relationships for star-schema-based reporting.

### **📈 Dashboard Features**
The **Healthcare Patient Flow Dashboard** provides insights into:
- **Admission Share / Occupancy Proxy** by department and gender *(see [Known Limitations](#️-known-limitations) — not a literal bed-capacity occupancy rate)*.
- **Patient Flow Trends** (admissions, wait times).
- **Department-Level KPIs** (length of stay, total patients, overstay counts).
- **Interactive Filters & Slicers** for gender.

<img width="1282" height="724" alt="Screenshot 2025-08-30 155951" src="https://github.com/user-attachments/assets/cf1f84dc-c1a5-4f07-84aa-1658abb4db16" />

---

## ⚠️ Known Limitations
Documenting these openly rather than glossing over them:
- **Occupancy metric is a proxy.** `vw_bed_occupancy` measures the share of recorded visits currently admitted, not admitted patients against real physical bed capacity — there is no `dim_bed_capacity` table in this model yet.
- **Gold refresh is full-rebuild batch**, not incremental streaming — see [Data Architecture](#-data-architecture) for the reasoning.
- **Synthetic data only.** The simulator generates fake patient events; this is a portfolio/learning pipeline, not connected to real hospital systems or PHI.
- Surrogate key generation uses an unpartitioned `row_number()` window, which is correct and collision-free but wouldn't scale to very high-volume incremental batches without further optimization.

---

## ✅ Key Outcomes
- **End-to-End Pipeline:** Real-time ingestion → streaming cleansing → dimensional modeling → serverless SQL → analytics.
- **Production-minded practices:** Secret scope credential management, data-quality flagging (not fabrication), collision-safe surrogate keys, Delta-aware SQL access.
- **Scalable Architecture:** Adaptable to different hospital datasets or additional KPIs.
- **Business Insights:** Hospital admins can monitor patient flow, department load, and length-of-stay trends.
- **Portfolio Value:** Demonstrates streaming data engineering, dimensional modeling (including SCD2), and BI analytics in a single, internally consistent project.

---

## 📜 License
This project is licensed under the **MIT License**.
Feel free to use and adapt for learning or production.
