# 🚑 MediFlowRT: Real-Time Healthcare Analytics Platform on Azure

![Azure](https://img.shields.io/badge/Azure-Cloud-0078D4?logo=microsoftazure&style=flat-square)
![Azure Event Hub](https://img.shields.io/badge/Azure-EventHub-0078D4?logo=microsoftazure&style=flat-square)
![Azure Data Lake](https://img.shields.io/badge/Azure-DataLake-0078D4?logo=microsoftazure&style=flat-square)
![Azure Synapse](https://img.shields.io/badge/Azure-Synapse%20Analytics-0078D4?logo=microsoftazure&style=flat-square)
![Databricks](https://img.shields.io/badge/Databricks-PySpark-EF3E42?logo=databricks&style=flat-square)
![PySpark](https://img.shields.io/badge/PySpark-Big%20Data-F37626?logo=apachespark&style=flat-square)
![Python](https://img.shields.io/badge/Python-3.9+-3776AB?logo=python&style=flat-square)
![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-F2C811?logo=powerbi&style=flat-square)
![Git](https://img.shields.io/badge/Git-Version%20Control-F05032?logo=git&style=flat-square)

---

# 🚀 Project Highlights

✔ End-to-End Azure Data Engineering Pipeline

✔ Real-Time Streaming with Azure Event Hub

✔ Medallion Architecture (Bronze → Silver → Gold)

✔ Azure Synapse SQL Data Warehouse

✔ Star Schema Data Modeling

✔ Interactive Power BI Dashboard

✔ Production-Oriented Repository Structure

---

# 📑 Table of Contents

- [📌 Project Overview](#-project-overview)
- [🎯 Objectives](#-objectives)
- [📂 Project Structure](#-project-structure)
- [🛠️ Tools & Technologies](#️-tools--technologies)
- [📐 Solution Architecture](#-solution-architecture)
- [⭐ Star Schema Design](#-star-schema-design)
- [⚙️ Step-by-Step Implementation](#️-step-by-step-implementation)
- [📊 Analytics Dashboard](#-analytics-dashboard)
- [💼 Business Value](#-business-value)
- [🎯 Skills Demonstrated](#-skills-demonstrated)
- [✅ Key Outcomes](#-key-outcomes)
- [📜 License](#-license)

---

# 📌 Project Overview

**MediFlowRT** is an end-to-end **real-time healthcare analytics platform** built on Microsoft Azure. The project simulates hospital patient events, ingests streaming healthcare data, processes and transforms it using Azure Databricks, stores curated datasets in Azure Synapse SQL Pool, and visualizes operational KPIs through Power BI.

The solution demonstrates modern cloud-native data engineering practices including real-time streaming, Medallion Architecture, dimensional modeling, and business intelligence reporting.

### Part 1 – Data Engineering

Build a scalable real-time ingestion and transformation pipeline using Azure cloud services.

### Part 2 – Business Analytics

Connect Azure Synapse SQL Pool to Power BI and develop an interactive dashboard for hospital operations and patient flow monitoring.

---

# 📐 Solution Architecture

The platform follows a modern cloud data engineering architecture:

- Simulate hospital patient events using Python.
- Stream data into Azure Event Hub.
- Store raw data in Azure Data Lake Storage Gen2.
- Process streaming data using Azure Databricks (PySpark).
- Transform data using the Bronze → Silver → Gold Medallion Architecture.
- Load curated datasets into Azure Synapse SQL Pool.
- Build an interactive Power BI dashboard for operational analytics.

## Pipeline Architecture

<img width="4719" height="2432" alt="Architecture" src="https://github.com/user-attachments/assets/cb1a1775-ab64-45d7-b45b-50ba97660e1d"/>

---

# 🎯 Objectives

- Build a real-time healthcare data pipeline on Azure.
- Ingest streaming patient data using Azure Event Hub.
- Process and transform streaming data using Azure Databricks (PySpark).
- Store curated datasets in Azure Data Lake Storage.
- Design a Star Schema in Azure Synapse SQL Pool.
- Enable interactive analytics using Power BI.
- Manage source code with Git and GitHub.

---

# 📂 Project Structure

```text
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
│   ├── SQL_views_DDL.sql
│   └── SQL_pool_queries.sql
│
├── client_requirements/
│
├── Hospital_Dashboard.pbix
│
├── README.md
│
└── LICENSE
```

---

# 🛠️ Tools & Technologies

| Technology | Purpose |
|------------|---------|
| Azure Event Hub | Real-time Data Ingestion |
| Azure Databricks | Stream Processing & ETL |
| PySpark | Distributed Data Processing |
| Azure Data Lake Storage Gen2 | Data Lake Storage |
| Azure Synapse SQL Pool | Data Warehouse |
| Power BI | Dashboard & Visualization |
| Python | Data Simulation |
| SQL | Data Modeling |
| Git & GitHub | Version Control |

---

# 📐 Data Architecture

The project follows the **Medallion Architecture** for scalable data engineering.

## 🥉 Bronze Layer

- Stores raw JSON events from Azure Event Hub.
- Maintains immutable source data.
- Serves as the ingestion layer.

## 🥈 Silver Layer

- Cleanses raw data.
- Handles missing values.
- Performs schema validation.
- Standardizes data types.

## 🥇 Gold Layer

- Creates business-ready datasets.
- Generates aggregated metrics.
- Builds dimensional tables.
- Optimized for Power BI reporting.

---

# ⭐ Star Schema Design

The Gold Layer is modeled using a Star Schema for high-performance analytical queries.

## Fact Table

**FactPatientFlow**

Contains:

- Patient Visits
- Admission Time
- Discharge Time
- Waiting Time
- Length of Stay

## Dimension Tables

- DimPatient
- DimDepartment
- DimTime

This dimensional model improves query performance and supports interactive reporting.

---

# ⚙️ Step-by-Step Implementation

## 1️⃣ Azure Event Hub

- Created Azure Event Hub Namespace.
- Configured Patient Flow Event Hub.
- Created Consumer Groups for Databricks Streaming.

---

## 2️⃣ Patient Data Simulation

A Python simulator continuously generates synthetic patient events and publishes them to Azure Event Hub.

Source Code:

`simulator/patient_flow_generator.py`

---

## 3️⃣ Azure Data Lake Storage

Configured Azure Data Lake Storage Gen2 with separate containers for:

- Bronze
- Silver
- Gold

---

## 4️⃣ Azure Databricks

### Bronze Notebook

Reads streaming data from Azure Event Hub.

### Silver Notebook

Performs:

- Schema Validation
- Data Cleansing
- Null Handling
- Type Casting

### Gold Notebook

Creates:

- Aggregated Metrics
- Dimension Tables
- Fact Tables
- Star Schema

---

## 5️⃣ Azure Synapse SQL Pool

Created a Dedicated SQL Pool.

Implemented:

- Fact Table
- Dimension Tables
- SQL Views
- Optimized Warehouse Schema

---

## 6️⃣ Version Control

Managed project development using Git and GitHub.

---

# 📊 Analytics Dashboard

After building the data pipeline and Star Schema, Azure Synapse SQL Pool was connected to Power BI to create an interactive healthcare dashboard.

## Dashboard Features

- Total Patients
- Bed Occupancy Rate
- Average Waiting Time
- Patient Admission Trends
- Department Performance
- Length of Stay
- Gender Distribution
- Interactive Filters & Slicers

<img width="1282" height="724" alt="Dashboard" src="https://github.com/user-attachments/assets/cf1f84dc-c1a5-4f07-84aa-1658abb4db16"/>

---

# 💼 Business Value

This solution enables hospital administrators to:

- Monitor patient flow in real time.
- Track department efficiency.
- Optimize bed utilization.
- Reduce patient waiting time.
- Improve operational decision-making.
- Support healthcare resource planning.

---

# 🎯 Skills Demonstrated

- Azure Cloud
- Azure Event Hub
- Azure Databricks
- PySpark
- Structured Streaming
- Azure Data Lake Storage Gen2
- Azure Synapse Analytics
- SQL
- Star Schema Design
- ETL Pipelines
- Data Warehousing
- Power BI
- Git & GitHub
- Python

---

# ✅ Key Outcomes

- Built a complete end-to-end Azure Data Engineering pipeline.
- Implemented Medallion Architecture (Bronze → Silver → Gold).
- Designed a scalable Star Schema in Azure Synapse SQL Pool.
- Developed an interactive Power BI dashboard for healthcare analytics.
- Demonstrated modern cloud-based data engineering and analytics practices suitable for enterprise-scale solutions.

---
## 📜 License

This repository is provided for learning, portfolio demonstration, and experimentation. It is licensed under the MIT License. See the LICENSE file for details.
