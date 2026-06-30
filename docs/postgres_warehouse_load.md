# PostgreSQL Warehouse Load

## 1. Objective

This document describes the process used to load Gold layer datasets and the Customer ABT into a local PostgreSQL warehouse.

The goal of this phase is to make analytics-ready data available through a relational database so it can be consumed by SQL clients, BI tools, and later dbt models.

---

## 2. Source Layer

The source datasets are located in:

```text
data/gold/
```

Datasets loaded:

```text
dim_customers
dim_products
dim_sellers
dim_dates
fact_orders
fact_order_items
fact_payments
fact_reviews
mart_sales_by_month
customer_abt
```

---

## 3. Target Warehouse

Target database:

```text
PostgreSQL
```

Target schema:

```text
gold
```

Local connection defaults:

```text
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=olist_warehouse
POSTGRES_USER=olist_user
POSTGRES_PASSWORD=olist_password
POSTGRES_SCHEMA=gold
```

---

## 4. Docker Service

PostgreSQL is started locally through Docker Compose.

File:

```text
docker-compose.postgres.yml
```

Command:

```powershell
docker compose -f docker-compose.postgres.yml up -d
```

---

## 5. Load Script

The warehouse load is performed by:

```text
scripts/warehouse/load_gold_to_postgres.py
```

The script:

1. Reads local Gold Parquet datasets.
2. Connects to PostgreSQL.
3. Creates the `gold` schema if it does not exist.
4. Loads each dataset as a PostgreSQL table.
5. Replaces existing tables during each load.

---

## 6. Validation Script

The warehouse validation is performed by:

```text
scripts/warehouse/validate_postgres_load.py
```

The script validates:

1. Table existence.
2. Row counts.
3. Basic query execution.
4. Customer ABT churn distribution.

---

## 7. Expected Row Counts

| Table | Expected Rows |
|---|---:|
| `gold.dim_customers` | 96,096 |
| `gold.dim_products` | 32,951 |
| `gold.dim_sellers` | 3,095 |
| `gold.dim_dates` | 634 |
| `gold.fact_orders` | 99,441 |
| `gold.fact_order_items` | 112,650 |
| `gold.fact_payments` | 99,440 |
| `gold.fact_reviews` | 98,673 |
| `gold.mart_sales_by_month` | 24 |
| `gold.customer_abt` | 96,096 |

---

## 8. Business Value

This phase turns the Data Lake outputs into a queryable warehouse layer.

The PostgreSQL warehouse enables:

- SQL exploration
- BI dashboard connectivity
- dbt modeling
- downstream reporting
- business-facing analytics

---

## 9. Next Steps

Recommended next phases:

```text
1. Build Power BI dashboards using PostgreSQL tables
2. Add Airflow orchestration
3. Refactor Gold transformations with dbt
4. Add data quality tests
5. Add Kafka streaming simulation
```
