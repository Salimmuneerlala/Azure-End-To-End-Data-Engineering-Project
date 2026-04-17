# End-to-End Azure Data Engineering Pipeline

### Medallion Architecture | Incremental ETL | Star Schema Modeling

---

## Project Summary

This project implements a **production-style data engineering pipeline** on Azure using the Medallion Architecture (Bronze, Silver, Gold).

The pipeline ingests transactional data, processes it using distributed computing, and transforms it into an **analytics-ready data warehouse** designed using a Star Schema.

> ⚡ Focus: Scalability, incremental processing, and clean data modeling practices

---

## Problem Statement

Organizations often struggle with:

* Handling growing volumes of raw data
* Maintaining clean and reliable datasets
* Enabling fast analytics without overloading transactional systems

This project solves these challenges by building a **layered data platform** that separates raw ingestion, transformation, and business-level modeling.

---

## Solution Architecture

```
GitHub → Azure SQL DB → ADF → ADLS (Bronze → Silver → Gold) → Databricks → Power BI
```

### Key Design Decisions:

* **Medallion Architecture** → Ensures data quality and maintainability
* **Incremental Loads (CDC)** → Avoids full reloads, improves efficiency
* **Star Schema (Gold Layer)** → Optimized for analytics queries
* **Unity Catalog** → Centralized governance and access control

---

## ⚙️ Tech Stack

| Layer         | Tools Used                 |
| ------------- | -------------------------- |
| Ingestion     | Azure Data Factory         |
| Source        | Azure SQL Database, GitHub |
| Storage       | ADLS Gen2                  |
| Processing    | Azure Databricks, PySpark  |
| Governance    | Unity Catalog              |
| Orchestration | ADF + Databricks Workflows |
| Visualization | Power BI / Databricks SQL  |

---

## 🔄 End-to-End Data Flow

### 1. Data Source

* Raw datasets hosted on GitHub
* Loaded into Azure SQL Database (simulating a transactional system)

### 2. Ingestion – Bronze Layer

* ADF pipelines extract data incrementally
* Parameterized pipelines enable reusability
* Data stored in raw format in ADLS

### 3. Transformation – Silver Layer

* PySpark used for:

  * Data cleaning
  * Handling nulls and duplicates
  * Schema standardization

### 4. Modeling – Gold Layer

* Designed using **Dimensional Modeling**
* Created:

  * Fact tables
  * Dimension tables
* Implemented:

  * Surrogate keys
  * Slowly Changing Dimensions (Type 1)

### 5. Orchestration

* ADF handles ingestion scheduling
* Databricks Workflows manage transformation pipelines

### 6. Consumption

* Gold layer serves analytics use cases
* Connected to Power BI for dashboards

---

## Medallion Architecture Explained

| Layer  | Purpose                        |
| ------ | ------------------------------ |
| Bronze | Raw, unprocessed data          |
| Silver | Cleaned, structured data       |
| Gold   | Business-level aggregated data |

---

## Project Structure

```id="p7n3xk"
project/
├── adf/
│   └── pipelines/
├── databricks/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── notebooks/
├── configs/
├── logs/
└── README.md
```

---

## Key Features

* ✅ Incremental data loading (CDC-based logic)
* ✅ Parameterized and reusable pipelines
* ✅ Distributed processing using PySpark
* ✅ Star Schema for optimized querying
* ✅ Workflow orchestration
* ✅ Data governance with Unity Catalog

---

## Sample Use Case

Business teams can:

* Analyze sales trends
* Track customer behavior
* Build dashboards without impacting source systems

---

## How to Run This Project

1. Provision Azure resources
2. Upload dataset to source (GitHub / SQL DB)
3. Configure ADF pipelines
4. Set up Databricks & Unity Catalog
5. Execute ingestion → transformation → modeling workflows

---

## Challenges & Learnings

* Designing **efficient incremental pipelines**
* Managing schema evolution across layers
* Structuring data for both flexibility and performance
* Understanding trade-offs between normalization vs denormalization

---

## Future Enhancements

* Implement SCD Type 2
* Add data quality validation (Great Expectations)
* CI/CD with Azure DevOps
* Monitoring & alerting (Azure Monitor)
* Delta Lake optimizations (Z-ordering, vacuuming)

---

## 📈 Why This Project Matters

This project demonstrates:

* Real-world data engineering patterns
* Cloud-native architecture design
* End-to-end ownership of data pipelines

---

## Acknowledgment

This project was inspired by a YouTube tutorial and extended with additional improvements and structure to reflect production-level practices.
