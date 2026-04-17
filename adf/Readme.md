# Azure Data Factory (ADF) Pipelines

This project uses Azure Data Factory (ADF) to orchestrate end-to-end data ingestion workflows for a scalable ETL pipeline.

---

## Overview

ADF acts as the **orchestration layer**, responsible for:

- Ingesting data from GitHub and Azure SQL Database  
- Managing incremental data loads using watermarking  
- Moving data into Azure Data Lake Storage (ADLS)  
- Enabling reusable and parameterized pipelines  

---

## ⚙️ Pipeline Design

The pipeline is designed using a **parameter-driven approach**, making it reusable, scalable, and avoiding hardcoded logic.

---

## 🔧 Key Components

---

### 1. Linked Services

Connections established between ADF and external systems:

- GitHub (source data)
- Azure SQL Database
- Azure Data Lake Storage (ADLS Gen2)

![Linked Services](adf/screenshots/01_linked_services.png)

---

### 2. Datasets

Defines structured data representations for source and destination systems.

- SQL datasets for relational data
- ADLS datasets for file storage

![Datasets](adf/screenshots/02_dataset_services.png)

---

### 3. Parameterized Datasets

Enables dynamic dataset usage across multiple tables.

- Supports reusable pipeline design  
- Eliminates hardcoding  

![Parameterized Dataset](adf/screenshots/03_dataset_parameterizing_.png)

---

## 🔄 Pipeline Flow

---

### 4. GitHub → Azure SQL (Initial Load)

#### Source
Extracts raw data from GitHub repository.

![GitHub Source](adf/screenshots/04_git_to_sql_copy_act_Source.png)

#### Sink
Loads data into Azure SQL Database.

![Azure SQL Sink](adf/screenshots/05_git_to_sql_copy_act_Sink.png)

---

### 5. SQL → ADLS (Bronze Layer)

Moves data from Azure SQL Database into Azure Data Lake Storage.

- Forms the Bronze (raw) layer of Medallion Architecture

![SQL to ADLS Pipeline](adf/screenshots/06_sql_to_adls_pipeline.png)

---

## 🔁 Incremental Load Implementation

---

### 6. Watermark Table (Last Load Tracking)

Stores last successful pipeline execution timestamp.

![Last Load Watermark](adf/screenshots/07_last_load_max_date.png)

---

### 7. Current Load Timestamp

Fetches latest available timestamp from source system.

![Current Load Timestamp](adf/screenshots/08_current_load_max_date.png)

---

### 8. Incremental Copy Activity

Implements incremental loading logic:

- Loads only new or updated records  
- Uses watermark-based filtering  

![Incremental Copy Activity](adf/screenshots/09_copy_data_incrementally.png)

---

### 9. Update Watermark Table

Updates watermark after successful execution:

- Ensures next run processes only new data  
- Maintains pipeline state  

![Update Watermark](adf/screenshots/10_update_last_load_max_date_St.png)

---

## 📌 Key Highlights

- Parameterized pipeline design  
- Incremental loading using watermark strategy  
- Reusable and scalable ETL workflow  
- Production-style orchestration design  

---

## 🧾 Summary

Azure Data Factory acts as the orchestration backbone for this project, enabling:

- Automated ingestion  
- Incremental processing  
- Scalable pipeline execution  
- Clean separation of ETL stages  
