# F1 Data Engineering Pipeline — AWS Glue, Snowflake, dbt & Airflow

> An end-to-end Formula 1 data engineering project that demonstrates batch ingestion, ETL processing, cloud orchestration, data warehousing, dimensional modelling, incremental transformations, and data-quality validation using **Amazon S3, AWS Glue, Apache Airflow (MWAA), Snowflake, and dbt**.


## Project Overview

This project builds a complete cloud-based data pipeline for Formula 1 data.

The pipeline starts with raw batch files stored in an **Amazon S3 landing zone**. Apache Airflow orchestrates the ingestion workflow and invokes **six AWS Glue ETL jobs** to process the individual F1 datasets.

The processed data is written back to Amazon S3 in **Parquet format**. A second Airflow DAG then loads the processed datasets into the **Bronze layer of Snowflake**.

Once the raw data is available in Snowflake, **dbt** performs the warehouse transformations:




The Gold layer is designed as an analytical star schema that supports downstream F1 analysis such as race results, driver performance, constructor performance, and season-level insights.

## Tech Stack

| Category | Technologies |
|---|---|
| **Programming Languages** | SQL, Python |
| **Cloud Platform** | AWS |
| **Storage** | Amazon S3 |
| **Data Processing** | AWS Glue, PySpark |
| **Orchestration** | Apache Airflow, Amazon MWAA |
| **Data Warehouse** | Snowflake |
| **Data Transformation** | dbt |
| **Access Management** | AWS IAM |
| **Version Control** | Git, GitHub |
| **Development Tools** | VS Code |

## Business Objective

The goal is to transform raw Formula 1 datasets into reliable, analytics-ready data.

The pipeline supports analysis across:

- Drivers
- Constructors
- Circuits
- Races
- Race results
- Sprint results
- Driver performance
- Constructor performance
- Race and season statistics
- Driver and constructor nationality regions

The project demonstrates how raw operational-style datasets can be transformed into a structured analytical warehouse.



# Architecture

## High-Level Architecture

<img width="1536" height="1024" alt="ChatGPT Image Sep 6, 2026, 06_50_53 PM" src="https://github.com/user-attachments/assets/4d236354-8308-45b9-b19f-2c4058289801" />


### Main architecture layers

| Layer | Technology | Responsibility |
|---|---|---|
| Source / Landing | Amazon S3 | Stores raw F1 batch files |
| Orchestration | Apache Airflow / MWAA | Coordinates ingestion and Snowflake loading |
| ETL | AWS Glue | Reads raw files, validates schemas and transforms data into processed Parquet |
| Processed Storage | Amazon S3 | Stores processed datasets |
| Data Warehouse | Snowflake | Stores Bronze, Silver and Gold data |
| Transformation | dbt | Builds Silver and Gold models |
| Analytics | Gold layer | Provides analytics-ready dimensional models |




# End-to-End Data Flow

The complete pipeline follows this sequence:

### Step 1 — Raw data arrives in S3

Raw Formula 1 files are stored in the S3 landing zone.

```text
S3
└── landing/
    └── 2025-01/
        ├── circuits.csv
        ├── constructors.json
        ├── drivers.json
        ├── races.csv
        ├── results/
        └── sprints/
```

### Step 2 — Airflow detects the latest batch

Airflow DAG 1 identifies the latest available batch in the S3 landing location.

### Step 3 — Batch validation

The pipeline validates the expected input files before processing.

### Step 4 — AWS Glue processes the datasets

Six Glue ETL jobs process the individual F1 datasets:

```text
circuits_ingestion
constructors_ingestion
drivers_ingestion
races_ingestion
results_ingestion
sprints_ingestion
```

### Step 5 — Processed data is written to S3

Glue writes the processed datasets to the S3 `processed/` zone in Parquet format.

### Step 6 — Airflow triggers the Snowflake loading workflow

After the ingestion jobs complete successfully, DAG 1 triggers the Snowflake loading DAG.

### Step 7 — Data is loaded into Snowflake Bronze

DAG 2 retrieves the batch ID and executes Snowflake load operations for the six datasets.

### Step 8 — dbt builds Silver models

dbt cleans and standardises the Bronze tables.

### Step 9 — dbt builds Gold models

dbt transforms the Silver layer into an analytical dimensional model.

### Step 10 — Analytics-ready data

The Gold layer provides a consistent structure for downstream analytics and BI.

---

# 1. S3 Landing Zone

Amazon S3 acts as the initial landing area for the raw F1 datasets.

<img width="1920" height="1080" alt="Screenshot (238)" src="https://github.com/user-attachments/assets/c2af1ff4-0758-4a92-9bd6-dc5c41c5a27d" />


The bucket contains separate `landing/` and `processed/` areas.

Example:

```text
f1-data-engineering-bucket/
│
├── landing/
│   └── 2025-01/
│       ├── circuits.csv
│       ├── constructors.json
│       ├── drivers.json
│       ├── races.csv
│       ├── results/
│       └── sprints/
│
└── processed/
    └── 2025-01/
        ├── circuits/
        ├── constructors/
        ├── drivers/
        ├── races/
        ├── results/
        └── sprints/
```

### Why S3 is used

- Durable cloud storage for raw files
- Clear separation between landing and processed data
- Batch-oriented processing
- Decouples source files from downstream processing
- Provides a persistent intermediate storage layer

---

# 2. Airflow DAG 1 — Ingestion & Processing

The first Airflow DAG is responsible for orchestrating the ingestion process.

**DAG:** `f1_pipeline`

<img width="1920" height="1080" alt="Screenshot (239)" src="https://github.com/user-attachments/assets/9685df86-31cb-4d05-b567-24ae5b53665c" />




### Main tasks

#### `detect_batch`

Identifies the latest available batch from the S3 landing zone.

#### `validate_batch`

Checks the incoming batch before starting the ETL processing.

#### Dataset ingestion tasks

The DAG runs six AWS Glue jobs:

- `circuits_ingestion`
- `constructors_ingestion`
- `drivers_ingestion`
- `races_ingestion`
- `results_ingestion`
- `sprints_ingestion`

#### `bronze_complete`

Acts as the completion point after all six ingestion jobs finish.

#### `trigger_snowflake_pipeline`

Triggers the second Airflow DAG responsible for loading the processed data into Snowflake.

---

# 3. AWS Glue ETL

AWS Glue performs the dataset-level ETL processing.

<img width="1920" height="1080" alt="Screenshot (240)" src="https://github.com/user-attachments/assets/f7bb748d-98fe-49d3-acf5-581206b8e8b2" />


Each dataset has its own Glue ingestion job.

### Glue jobs

| Glue Job | Dataset |
|---|---|
| `f1_circuits_ingestion` | Circuits |
| `f1_constructors_ingestion` | Constructors |
| `f1_drivers_ingestion` | Drivers |
| `f1_races_ingestion` | Races |
| `f1_results_ingestion` | Results |
| `f1_sprints_ingestion` | Sprints |

The Glue scripts use **PySpark** to process the input data.

The ETL layer handles activities such as:

- Reading files from S3
- Applying expected schemas
- Data type handling
- Dataset-specific transformations
- Writing processed output
- Producing Parquet datasets

---

# 4. S3 Processed Zone

After Glue processing, the transformed datasets are stored in the S3 processed zone.

```text
processed/
└── 2025-01/
    ├── circuits/
    ├── constructors/
    ├── drivers/
    ├── races/
    ├── results/
    └── sprints/
```

The processed files are stored in **Parquet format**, providing a structured and efficient intermediate representation before loading into Snowflake.

---

# 5. Airflow DAG 2 — Snowflake Loading

The second DAG is responsible for loading the processed datasets into Snowflake.

**DAG:** `f1_snowflake_pipeline`

<img width="1920" height="1080" alt="Screenshot (241)" src="https://github.com/user-attachments/assets/95e0b472-8cc3-4062-b8c9-118777e12fdf" />




The loading tasks execute Snowflake SQL through Airflow.

### Main responsibilities

- Retrieve the batch ID
- Identify the processed batch
- Load processed files into Snowflake
- Populate Bronze tables
- Maintain batch-aware processing

The six primary Bronze datasets are:

```text
CIRCUITS
CONSTRUCTORS
DRIVERS
RACES
RESULTS
SPRINTS
```

---

# 6. Snowflake Data Warehouse

Snowflake is the central analytical data warehouse for the project.

<img width="1920" height="1080" alt="Screenshot (242)" src="https://github.com/user-attachments/assets/5dd608a9-a879-4195-b2d2-ce6d668142a4" />


The warehouse is organised into separate logical layers:

```text
F1_DATABASE
│
├── BRONZE
│   ├── CIRCUITS
│   ├── CONSTRUCTORS
│   ├── DRIVERS
│   ├── RACES
│   ├── RESULTS
│   └── SPRINTS
│
├── SILVER
│   ├── CIRCUITS
│   ├── CONSTRUCTORS
│   ├── DRIVERS
│   ├── RACES
│   ├── RESULTS
│   └── SPRINTS
│
└── GOLD
    ├── DIM_CONSTRUCTORS
    ├── DIM_DRIVERS
    ├── DIM_RACES
    ├── FACT_SESSION_RESULTS
    └── REF_NATIONALITY_REGION
```

---

# 7. dbt Transformations

dbt is responsible for the transformation layer inside Snowflake.

<img width="1920" height="1080" alt="Screenshot (243)" src="https://github.com/user-attachments/assets/96cdf124-c871-4bfc-896c-1d49b6de363c" />


The dbt project contains separate model directories for:

```text
models/
├── silver/
│   ├── circuits.sql
│   ├── constructors.sql
│   ├── drivers.sql
│   ├── races.sql
│   ├── results.sql
│   └── sprints.sql
│
└── gold/
    ├── dim_constructors.sql
    ├── dim_drivers.sql
    ├── dim_races.sql
    ├── fact_session_results.sql
    └── ref_nationality_region.sql
```

dbt uses `ref()` to create dependencies between models and supports incremental processing for the analytical models.

Example configuration:

```sql
{{ config(
    materialized='incremental',
    unique_key='driver_id',
    incremental_strategy='merge'
) }}
```

This allows the model to process new batches without rebuilding the entire target table.

---

# 8. Bronze Layer

The Bronze layer represents the raw data as delivered into Snowflake.

<img width="1052" height="753" alt="image" src="https://github.com/user-attachments/assets/76717f91-c679-47a9-af21-4bd92fa14cb4" />


The Bronze tables preserve the original source-style structure and field naming.

Examples include:

```text
circuits
constructors
drivers
races
results
sprints
```

The Bronze layer is intentionally close to the incoming source structure so that the original data remains available for traceability and downstream transformation.

### Bronze principles

- Preserve source information
- Enforce expected schemas during ingestion
- Keep the raw warehouse representation
- Maintain a reliable starting point for dbt transformations

---

# 9. Silver Layer

The Silver layer contains cleaned and standardised data.

<img width="1050" height="752" alt="image" src="https://github.com/user-attachments/assets/7f021090-505c-455d-bb08-273659a525d6" />


dbt transforms the Bronze data by applying consistent naming, data cleaning and business-friendly structures.

### Examples of standardisation

```text
lat        → latitude
lng        → longitude
date       → race_date
grid       → grid_position
```

### Silver transformations include

- Snake-case column naming
- Meaningful column names
- Data type standardisation
- Business-key validation
- Duplicate removal
- Text standardisation
- Lineage metadata

The Silver tables also carry metadata such as:

```text
ingestion_timestamp
source_file
batch_id
```

These attributes improve lineage and batch-level traceability.

---

# 10. Gold Layer

The Gold layer reshapes the Silver data into a dimensional analytical model.

<img width="1052" height="478" alt="image" src="https://github.com/user-attachments/assets/494c9b88-3d16-4db6-8022-db3c4643a486" />


The central fact table is:

```text
FACT_SESSION_RESULTS
```

and it is supported by the following dimensions/reference model:

```text
DIM_RACES
DIM_DRIVERS
DIM_CONSTRUCTORS
REF_NATIONALITY_REGION
```

# Data Quality & Traceability

Data quality is addressed at multiple stages of the pipeline.

## Input validation

Airflow validates the incoming batch before Glue processing begins.

## Schema enforcement

Glue ingestion jobs use expected schemas and fail loudly when the incoming structure does not match expectations.

## Silver quality rules

The Silver layer applies cleaning and standardisation such as:

- Null checks on business keys
- Duplicate removal
- Data type conversion
- Standardised naming
- Text normalisation

## dbt testing

dbt tests are used as part of the transformation process to validate the transformed data.

## Lineage metadata

The Silver models carry:

```text
ingestion_timestamp
source_file
batch_id
```

This allows individual records to be traced back to their source batch and file.

---

# Project Structure

A representative repository structure is:

```text
F1-Data-Engineering-Pipeline-AWS-Glue-Snowflake-dbt-Airflow/
│
├── Data_Ingestion-Bronze_Layer/
│   ├── Glue scripts
│   └── ingestion logic
│
├── f1_dbt/
│   ├── models/
│   │   ├── silver/
│   │   │   ├── circuits.sql
│   │   │   ├── constructors.sql
│   │   │   ├── drivers.sql
│   │   │   ├── races.sql
│   │   │   ├── results.sql
│   │   │   └── sprints.sql
│   │   │
│   │   └── gold/
│   │       ├── dim_constructors.sql
│   │       ├── dim_drivers.sql
│   │       ├── dim_races.sql
│   │       ├── fact_session_results.sql
│   │       └── ref_nationality_region.sql
│   │
│   ├── macros/
│   ├── seeds/
│   ├── snapshots/
│   ├── analyses/
│   ├── dbt_project.yml
│   ├── schema.yml
│   └── sources.yml
│
├── airflow/
│   ├── f1_pipeline.py
│   └── f1_snowflake_pipeline.py
│
└── docs/
    └── images/
```

> The exact repository structure may vary depending on how deployment files and scripts are organised.

---

# Key Engineering Concepts Demonstrated

This project demonstrates practical data engineering concepts across the complete pipeline.

### Cloud storage

- Amazon S3
- Landing and processed zones
- Batch-oriented data organisation

### Orchestration

- Apache Airflow
- Airflow DAG dependencies
- Task sequencing
- Glue job orchestration
- Triggering a downstream DAG

### ETL

- AWS Glue
- PySpark
- Schema enforcement
- Dataset-specific transformations
- Parquet output

### Data warehousing

- Snowflake
- Bronze / Silver / Gold architecture
- Warehouse schemas
- Fact and dimension modelling

### Analytics engineering

- dbt
- `ref()` dependencies
- Incremental models
- Merge strategy
- dbt tests
- Dimensional modelling

### Data quality

- Batch validation
- Schema validation
- Duplicate handling
- Null/business-key checks
- Transformation testing

### Data lineage

- Batch IDs
- Source file metadata
- Ingestion timestamps

---

# Pipeline Design Principles

The project follows several practical data engineering principles:

## Separation of concerns

Each technology has a focused responsibility:

```text
S3       → Storage
Airflow  → Orchestration
Glue     → ETL / Processing
Snowflake → Data Warehouse
dbt      → Transformation / Modelling
```

## Layered architecture

The data is progressively refined:

```text
Raw
 ↓
Bronze
 ↓
Silver
 ↓
Gold
```

Each layer has a clear purpose rather than mixing ingestion, cleaning and analytics logic together.

## Batch-aware processing

The pipeline uses a batch ID to identify and process individual data deliveries.

## Incremental transformations

dbt models use incremental strategies to avoid rebuilding the entire analytical dataset for every new batch.

## Reusable analytical models

Business logic such as driver/constructor nationality-region mapping is centralised rather than duplicated across downstream queries.

---

# Future Analytics

The Gold layer provides a foundation for downstream analytics such as:

- Race result analysis
- Driver performance analysis
- Constructor performance
- Podium and win statistics
- Points analysis
- Season comparisons
- Sprint vs race performance
- Geographic analysis by nationality region

A BI layer can be connected to the Gold schema for dashboards and reporting.

---

# Conclusion

The **F1 Data Engineering Pipeline** demonstrates an end-to-end modern data engineering workflow using AWS, Snowflake, Airflow and dbt.

The pipeline takes raw Formula 1 batch files from an S3 landing zone, processes them through AWS Glue, stores the processed data as Parquet, loads the datasets into Snowflake Bronze through Airflow, and progressively transforms the data through dbt Silver and Gold layers.



**Amazon S3 · AWS Glue · PySpark · Apache Airflow / MWAA · Snowflake · dbt · SQL · Python**

