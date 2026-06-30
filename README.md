# Olist Data Engineering Project

## Overview

This project builds an end-to-end Data Engineering pipeline using the **Olist Brazilian E-Commerce dataset**.

The goal is to design and implement a modern data platform that demonstrates practical Data Engineering, Analytics Engineering, and data platform skills.

The project will progressively include:

- Python
- PySpark
- Data Lake architecture
- Medallion architecture
- Parquet
- Delta Lake
- Apache Airflow
- dbt
- Apache Kafka
- PostgreSQL
- Power BI
- Data quality checks
- Analytical Base Table generation

---

## Current Status

Completed phases:

- Project structure setup
- Raw data profiling
- Data profiling documentation
- Bronze layer ingestion
- Bronze layer validation

Current layer status:

```text
Raw CSV files → Bronze Parquet datasets
```

Next phase:

```text
Silver Layer — data cleaning, standardization, validation, and business-level transformations
```

---

## Dataset

This project uses the **Olist Brazilian E-Commerce dataset**.

The raw data should be placed in:

```text
data/raw/olist/
```

Expected raw files:

```text
olist_customers_dataset.csv
olist_geolocation_dataset.csv
olist_order_items_dataset.csv
olist_order_payments_dataset.csv
olist_order_reviews_dataset.csv
olist_orders_dataset.csv
olist_products_dataset.csv
olist_sellers_dataset.csv
product_category_name_translation.csv
```

> Note: Raw data files are not tracked in Git. Download the dataset separately and place the CSV files into the expected raw data folder.

---

## Project Structure

```text
Proyecto_DE/
│
├── data/
│   ├── raw/
│   │   └── olist/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── streaming/
│
├── docs/
│   ├── data_profiling_report.md
│   └── bronze_layer_validation.md
│
├── scripts/
│   └── profiling/
│
├── spark_jobs/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── abt/
│
├── airflow/
│   └── dags/
│
├── dbt/
│
├── kafka/
│   ├── producers/
│   └── consumers/
│
├── sql/
│   ├── ddl/
│   ├── dml/
│   └── queries/
│
├── docker/
├── dashboards/
├── tests/
├── logs/
│
├── README.md
├── requirements.txt
├── docker-compose.yml
└── .gitignore
```

---

## Architecture

The project follows a Medallion Architecture:

```text
Raw Data
   ↓
Bronze Layer
   ↓
Silver Layer
   ↓
Gold Layer
   ↓
Data Warehouse / BI / ABT
```

### Bronze Layer

The Bronze layer stores raw source data in an optimized columnar format while preserving the original structure as much as possible.

Current Bronze features:

- Reads raw CSV files from `data/raw/olist/`
- Writes Parquet datasets to `data/bronze/`
- Adds technical metadata columns
- Validates row counts and metadata presence

Technical metadata columns:

```text
_ingestion_timestamp
_source_file
_source_table
_layer
```

### Silver Layer

The Silver layer will include:

- Data cleaning
- Type standardization
- Data quality flags
- Payment aggregation
- Geolocation aggregation
- Product metadata handling
- Order logistics validation

### Gold Layer

The Gold layer will include analytics-ready tables such as:

```text
dim_customers
dim_products
dim_sellers
dim_dates
fact_orders
fact_order_items
fact_payments
fact_reviews
customer_abt
```

---

## Bronze Layer Scripts

### Bronze ingestion

```text
spark_jobs/bronze/load_raw_to_bronze.py
```

This script:

1. Reads all raw Olist CSV files.
2. Adds technical metadata columns.
3. Writes each table as a Parquet dataset.
4. Stores the output in `data/bronze/`.

Run it with:

```powershell
python spark_jobs/bronze/load_raw_to_bronze.py
```

### Bronze validation

```text
spark_jobs/bronze/validate_bronze.py
```

This script validates:

1. Row counts per Bronze table.
2. Presence of required metadata columns.
3. Basic readability of each Parquet dataset.

Run it with:

```powershell
python spark_jobs/bronze/validate_bronze.py
```

---

## Bronze Validation Summary

| Table | Expected Rows | Actual Rows | Count Status | Metadata Status |
|---|---:|---:|---|---|
| `customers` | 99,441 | 99,441 | OK | OK |
| `geolocation` | 1,000,163 | 1,000,163 | OK | OK |
| `order_items` | 112,650 | 112,650 | OK | OK |
| `payments` | 103,886 | 103,886 | OK | OK |
| `reviews` | 99,224 | 99,224 | OK | OK |
| `orders` | 99,441 | 99,441 | OK | OK |
| `products` | 32,951 | 32,951 | OK | OK |
| `sellers` | 3,095 | 3,095 | OK | OK |
| `category_translation` | 71 | 71 | OK | OK |

---

## Documentation

Available documentation:

```text
docs/data_profiling_report.md
docs/bronze_layer_validation.md
```

### `data_profiling_report.md`

Contains the initial raw data profiling results, including:

- Table volumes
- Candidate primary keys
- Null analysis
- Relationship checks
- Temporal profile
- Data quality issues
- Initial technical decisions

### `bronze_layer_validation.md`

Documents the Bronze validation process and confirms that all Bronze datasets were successfully created and validated.

---

## Environment Setup

### Create virtual environment

```powershell
python -m venv .venv
```

### Activate virtual environment

```powershell
.venv\Scripts\Activate.ps1
```

### Install dependencies

```powershell
pip install -r requirements.txt
```

---

## Initial Requirements

Current dependencies:

```text
pyspark==3.5.1
pandas==2.2.2
pyarrow==16.1.0
```

---

## Windows Notes for PySpark

When running Spark locally on Windows, Hadoop utilities may be required.

A local Hadoop folder can be configured as:

```text
C:\hadoop\in\winutils.exe
C:\hadoop\in\hadoop.dll
```

Recommended environment variables:

```text
HADOOP_HOME=C:\hadoop
PATH includes C:\hadoop\in
```

This is only required for local Windows execution. When the project is later containerized with Docker, this workaround should no longer be necessary.

---

## Git Ignore Policy

The repository should track code and documentation, not heavy local data files.

The following paths should be ignored:

```text
.venv/
__pycache__/
*.pyc
.env
data/raw/
data/bronze/
data/silver/
data/gold/
data/streaming/
logs/
```

---

## Next Phase

The next development phase is:

```text
FASE 3 — Silver Layer
```

Silver Layer goals:

- Clean and standardize Bronze datasets
- Add data quality flags
- Aggregate payments by `order_id`
- Aggregate geolocation by zip code prefix
- Handle missing product categories
- Create logistics completeness flags
- Prepare data for Gold modeling

Target script:

```text
spark_jobs/silver/build_silver_layer.py
```

---

## Long-Term Roadmap

Planned future phases:

1. Silver Layer transformations
2. Gold dimensional model
3. Customer Analytical Base Table
4. Airflow orchestration
5. PostgreSQL warehouse
6. dbt models and tests
7. Kafka streaming simulation
8. Data quality checks
9. Power BI dashboards
10. Docker-based local platform
11. Cloud deployment option

---

## Project Goal

The final goal is to build a portfolio-ready Data Engineering platform that demonstrates:

- Batch processing with PySpark
- Lakehouse-style architecture
- Data modeling
- Data quality thinking
- Pipeline orchestration
- Analytics Engineering with dbt
- Streaming simulation with Kafka
- BI-ready outputs
- ABT generation for advanced analytics or machine learning
