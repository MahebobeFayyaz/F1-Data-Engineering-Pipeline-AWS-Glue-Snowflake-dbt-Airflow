# F1 Data Engineering Pipeline — AWS Glue, Snowflake, dbt & Airflow

> An end-to-end Formula 1 data engineering project that demonstrates batch ingestion, ETL processing, cloud orchestration, data warehousing, dimensional modelling, incremental transformations, and data-quality validation using **Amazon S3, AWS Glue, Apache Airflow (MWAA), Snowflake, and dbt**.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Business Objective](#business-objective)
- [Architecture](#architecture)
- [Technology Stack](#technology-stack)
- [End-to-End Data Flow](#end-to-end-data-flow)
- [1. S3 Landing Zone](#1-s3-landing-zone)
- [2. Airflow DAG 1 — Ingestion & Processing](#2-airflow-dag-1--ingestion--processing)
- [3. AWS Glue ETL](#3-aws-glue-etl)
- [4. S3 Processed Zone](#4-s3-processed-zone)
- [5. Airflow DAG 2 — Snowflake Loading](#5-airflow-dag-2--snowflake-loading)
- [6. Snowflake Data Warehouse](#6-snowflake-data-warehouse)
- [7. dbt Transformations](#7-dbt-transformations)
- [8. Bronze Layer](#8-bronze-layer)
- [9. Silver Layer](#9-silver-layer)
- [10. Gold Layer](#10-gold-layer)
- [Data Model](#data-model)
- [Incremental Processing](#incremental-processing)
- [Data Quality & Traceability](#data-quality--traceability)
- [Project Structure](#project-structure)
- [Key Engineering Concepts Demonstrated](#key-engineering-concepts-demonstrated)
- [Future Analytics](#future-analytics)
- [Screenshots](#screenshots)
- [Conclusion](#conclusion)

---

## Project Overview

This project builds a complete cloud-based data pipeline for Formula 1 data.

The pipeline starts with raw batch files stored in an **Amazon S3 landing zone**. Apache Airflow orchestrates the ingestion workflow and invokes **six AWS Glue ETL jobs** to process the individual F1 datasets.

The processed data is written back to Amazon S3 in **Parquet format**. A second Airflow DAG then loads the processed datasets into the **Bronze layer of Snowflake**.

Once the raw data is available in Snowflake, **dbt** performs the warehouse transformations:

```text
S3 Landing
    ↓
Airflow DAG 1
    ↓
AWS Glue ETL
    ↓
S3 Processed / Parquet
    ↓
Airflow DAG 2
    ↓
Snowflake Bronze
    ↓
dbt Silver
    ↓
dbt Gold
```

The Gold layer is designed as an analytical star schema that supports downstream F1 analysis such as race results, driver performance, constructor performance, and season-level insights.

---

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

---

# Architecture

## High-Level Architecture

![F1 Data Engineering Pipeline Architecture](docs/images/f1-pipeline-architecture.png)

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

---

## Architecture Design Reference

The following reference illustrates the general cloud data-pipeline architecture pattern used as inspiration for the project architecture.

![Architecture design reference](docs/images/architecture-reference.png)

> The reference image is included for documentation context only. The actual implementation is represented by the F1 pipeline architecture above.

---

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

![Amazon S3 bucket](docs/images/s3-bucket.png)

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

![Airflow DAG 1](docs/images/airflow-dag1.png)

## DAG 1 flow

```text
detect_batch
     ↓
validate_batch
     ↓
 ┌───────────────┬──────────────────┬─────────────────┐
 ↓               ↓                  ↓                 ↓
circuits      constructors       drivers           races
 ↓               ↓                  ↓                 ↓
results       sprints
 └───────────────┴──────────────────┴─────────────────┘
                         ↓
                  bronze_complete
                         ↓
              trigger_snowflake_pipeline
```

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

![AWS Glue ETL job](docs/images/aws-glue-job.png)

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

![Airflow DAG 2](docs/images/airflow-dag2.png)

## DAG 2 flow

```text
get_batch_id
      ↓
 ┌───────────────┬────────────────┬────────────────┐
 ↓               ↓                ↓                ↓
load_bronze_   load_bronze_    load_bronze_    load_bronze_
drivers        races           results          sprints
 ↓               ↓                ↓                ↓
load_bronze_circuits
 ↓
load_bronze_constructors
```

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

![Snowflake schemas](docs/images/snowflake-schemas.png)

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

![dbt model implementation](docs/images/gold-layer.png)

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

![Bronze Layer](docs/images/bronze-layer.png)

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

![Silver Layer](docs/images/silver-layer.png)

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

![Gold Layer](docs/images/gold-layer.png)

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

## Gold schema

```text
                   DIM_RACES
                       │
                       │
                       ▼
              FACT_SESSION_RESULTS
                 ▲              ▲
                 │              │
                 │              │
          DIM_DRIVERS     DIM_CONSTRUCTORS
```

### `FACT_SESSION_RESULTS`

Combines race and sprint session results and identifies the session using:

```text
session_type
```

It contains analytical measures and attributes such as:

- Grid position
- Completed laps
- Car number
- Points
- Final position
- Final position text
- Status
- `is_win`
- `is_podium`
- `has_points`

### `DIM_RACES`

Provides race-level descriptive information:

- Season
- Round
- Race name
- Race date
- Circuit name
- Locality
- Country

### `DIM_DRIVERS`

Provides driver attributes:

- Driver ID
- Driver name
- Date of birth
- Nationality
- Nationality region

### `DIM_CONSTRUCTORS`

Provides constructor attributes:

- Constructor ID
- Constructor name
- Nationality
- Nationality region

### `REF_NATIONALITY_REGION`

A reference mapping that groups driver and constructor nationalities into broader regions such as:

```text
Europe
Americas
Asia
Africa
Oceania
```

This avoids repeatedly implementing the same geographic classification logic in downstream models.

---

# Data Model

## Bronze

```text
CIRCUITS
CONSTRUCTORS
DRIVERS
RACES
RESULTS
SPRINTS
```

## Silver

```text
CIRCUITS
CONSTRUCTORS
DRIVERS
RACES
RESULTS
SPRINTS
```

The Silver layer maintains the source entities while improving consistency, quality and usability.

## Gold

```text
DIM_RACES
       │
       │
       ▼
FACT_SESSION_RESULTS
       ▲              ▲
       │              │
DIM_DRIVERS     DIM_CONSTRUCTORS
```

This structure follows a **star-schema approach**, with the fact table at the centre and descriptive dimensions around it.

---

# Incremental Processing

The dbt Gold models are designed to support incremental processing.

For example:

```sql
{{ config(
    materialized='incremental',
    unique_key='driver_id',
    incremental_strategy='merge'
) }}
```

The transformation logic uses the latest available `batch_id` to identify new data.

Conceptually:

```text
Existing Gold data
        +
New Bronze/Silver batch
        ↓
Incremental dbt transformation
        ↓
MERGE
        ↓
Updated Gold model
```

This approach reduces unnecessary full-table processing as additional batches arrive.

---

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

# Screenshots

## Architecture

![F1 pipeline architecture](docs/images/f1-pipeline-architecture.png)

## S3 Landing & Processed Storage

![S3 bucket](docs/images/s3-bucket.png)

## AWS Glue

![AWS Glue](docs/images/aws-glue-job.png)

## Airflow DAG 1

![Airflow DAG 1](docs/images/airflow-dag1.png)

## Airflow DAG 2

![Airflow DAG 2](docs/images/airflow-dag2.png)

## Snowflake

![Snowflake schemas](docs/images/snowflake-schemas.png)

## Bronze Layer

![Bronze layer](docs/images/bronze-layer.png)

## Silver Layer

![Silver layer](docs/images/silver-layer.png)

## Gold Layer

![Gold layer](docs/images/gold-layer.png)

---

# Conclusion

The **F1 Data Engineering Pipeline** demonstrates an end-to-end modern data engineering workflow using AWS, Snowflake, Airflow and dbt.

The pipeline takes raw Formula 1 batch files from an S3 landing zone, processes them through AWS Glue, stores the processed data as Parquet, loads the datasets into Snowflake Bronze through Airflow, and progressively transforms the data through dbt Silver and Gold layers.

The final Gold layer provides a clean dimensional model suitable for analytical workloads and future BI reporting.

```text
                 F1 RAW DATA
                     │
                     ▼
              ┌─────────────┐
              │   Amazon S3 │
              │   Landing   │
              └──────┬──────┘
                     │
                     ▼
              ┌─────────────┐
              │  Airflow    │
              │   DAG 1     │
              └──────┬──────┘
                     │
                     ▼
              ┌─────────────┐
              │  AWS Glue   │
              │  6 ETL Jobs │
              └──────┬──────┘
                     │
                     ▼
              ┌─────────────┐
              │   S3        │
              │  Processed  │
              │  Parquet    │
              └──────┬──────┘
                     │
                     ▼
              ┌─────────────┐
              │  Airflow    │
              │   DAG 2     │
              └──────┬──────┘
                     │
                     ▼
              ┌─────────────┐
              │  Snowflake  │
              │   Bronze    │
              └──────┬──────┘
                     │
                     ▼
              ┌─────────────┐
              │     dbt     │
              │   Silver    │
              └──────┬──────┘
                     │
                     ▼
              ┌─────────────┐
              │     dbt     │
              │    Gold     │
              └──────┬──────┘
                     │
                     ▼
              ANALYTICS-READY
                   DATA
```

---

## Technologies

**Amazon S3 · AWS Glue · PySpark · Apache Airflow / MWAA · Snowflake · dbt · SQL · Python**

