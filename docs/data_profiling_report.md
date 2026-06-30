# Data Profiling Report — Olist E-Commerce Dataset

## 1. Purpose

This document summarizes the initial data profiling process performed on the **Olist Brazilian E-Commerce dataset**.

The goal of this profiling phase is to understand the raw data before designing and implementing the Data Engineering pipeline. This includes analyzing table volumes, schemas, candidate primary keys, null values, relationship consistency, temporal coverage, and potential data quality issues.

This report will guide the design of the following layers:

```text
Bronze → Silver → Gold
```

It will also support future decisions related to:

- Data Lake architecture
- Data quality rules
- Dimensional modeling
- Analytical Base Table design
- Batch and streaming pipelines
- dbt models
- Airflow orchestration

---

## 2. Dataset Overview

The dataset contains e-commerce transactional data from Olist, including orders, customers, products, sellers, payments, reviews, and geolocation information.

The raw files analyzed were:

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

Raw data location:

```text
data/raw/olist/
```

---

## 3. Table Volume Summary

| Table | Rows | Columns | Description |
|---|---:|---:|---|
| `customers` | 99,441 | 5 | Customer information associated with orders |
| `geolocation` | 1,000,163 | 5 | Geolocation information by zip code prefix |
| `order_items` | 112,650 | 7 | Product-level order line items |
| `payments` | 103,886 | 5 | Payment information associated with orders |
| `reviews` | 99,224 | 7 | Customer review scores and comments |
| `orders` | 99,441 | 8 | Core order table |
| `products` | 32,951 | 9 | Product catalog information |
| `sellers` | 3,095 | 4 | Seller information |
| `category_translation` | 71 | 2 | Product category translation table |

---

## 4. Candidate Primary Keys

### 4.1 `customers`

Candidate key:

```text
customer_id
```

Findings:

- `customer_id` has 99,441 distinct values.
- `customer_unique_id` has 96,096 distinct values.

Interpretation:

`customer_id` appears to be unique at the order/customer relationship level, while `customer_unique_id` represents the real customer identity across multiple orders.

Decision:

- Use `customer_id` to join `customers` with `orders`.
- Use `customer_unique_id` for customer-level analytics and ABT construction.

---

### 4.2 `orders`

Candidate key:

```text
order_id
```

Findings:

- `order_id` has 99,441 distinct values.
- `customer_id` also has 99,441 distinct values in the `orders` table.

Interpretation:

In this dataset, each order is associated with one `customer_id`. However, customer behavior analysis should not be done using `customer_id`, because repeat customers are represented through `customer_unique_id`.

---

### 4.3 `products`

Candidate key:

```text
product_id
```

Findings:

- `product_id` has 32,951 distinct values.
- This matches the total number of product rows.

---

### 4.4 `sellers`

Candidate key:

```text
seller_id
```

Findings:

- `seller_id` has 3,095 distinct values.
- This matches the total number of seller rows.

---

### 4.5 `category_translation`

Candidate key:

```text
product_category_name
```

Findings:

- `product_category_name` has 71 distinct values.
- `product_category_name_english` also has 71 distinct values.

---

## 5. Table-Level Findings

## 5.1 Customers

Table:

```text
customers
```

Summary:

- Rows: 99,441
- Columns: 5
- Null values: none detected
- Distinct `customer_id`: 99,441
- Distinct `customer_unique_id`: 96,096
- Distinct `customer_state`: 27

Important columns:

```text
customer_id
customer_unique_id
customer_zip_code_prefix
customer_city
customer_state
```

Interpretation:

The difference between `customer_id` and `customer_unique_id` is critical. Some customers placed more than one order, which means `customer_unique_id` should be used when analyzing customer behavior over time.

Decision:

Use `customer_unique_id` as the main entity for the customer ABT.

---

## 5.2 Geolocation

Table:

```text
geolocation
```

Summary:

- Rows: 1,000,163
- Columns: 5
- Null values: none detected
- Distinct `geolocation_zip_code_prefix`: 19,015
- Distinct `geolocation_state`: 27

Important columns:

```text
geolocation_zip_code_prefix
geolocation_lat
geolocation_lng
geolocation_city
geolocation_state
```

Interpretation:

This table contains multiple latitude and longitude records per zip code prefix. It should not be used directly as a dimension table without aggregation.

Decision:

Create an aggregated geolocation table in the Silver layer.

Suggested Silver table:

```text
silver_geolocation_zipcode
```

Suggested columns:

```text
zip_code_prefix
avg_lat
avg_lng
city
state
```

---

## 5.3 Order Items

Table:

```text
order_items
```

Summary:

- Rows: 112,650
- Columns: 7
- Null values: none detected
- Distinct `order_id`: 98,666
- Distinct `product_id`: 32,951
- Distinct `seller_id`: 3,095
- Maximum observed items per order: 21

Important columns:

```text
order_id
order_item_id
product_id
seller_id
shipping_limit_date
price
freight_value
```

Interpretation:

An order can contain multiple items. This table is essential for sales, product, and seller analytics.

Decision:

Use this table as the base for:

```text
fact_order_items
fact_sales
product_performance
seller_performance
```

---

## 5.4 Payments

Table:

```text
payments
```

Summary:

- Rows: 103,886
- Columns: 5
- Null values: none detected
- Distinct `order_id`: 99,440
- Distinct `payment_type`: 5
- Distinct `payment_installments`: 24

Important columns:

```text
order_id
payment_sequential
payment_type
payment_installments
payment_value
```

Interpretation:

There are more payment rows than orders, which means one order may have multiple payment records.

Decision:

Payments should be aggregated at `order_id` level before being joined with orders.

Suggested aggregated metrics:

```text
total_payment_value
payment_count
max_payment_installments
main_payment_type
```

---

## 5.5 Reviews

Table:

```text
reviews
```

Summary:

- Rows: 99,224
- Columns: 7
- Distinct `review_id`: 98,410
- Distinct `order_id`: 98,673
- Distinct `review_score`: 5

Null values:

| Column | Nulls | Percentage |
|---|---:|---:|
| `review_comment_title` | 87,656 | 88.34% |
| `review_comment_message` | 58,247 | 58.70% |

Important columns:

```text
review_id
order_id
review_score
review_comment_title
review_comment_message
review_creation_date
review_answer_timestamp
```

Interpretation:

Review comments are highly incomplete, but `review_score` is available and useful for customer satisfaction analysis.

Decision:

- Keep reviews even if comment fields are null.
- Use `review_score` as the primary customer satisfaction metric.
- Handle duplicated reviews by `order_id` in a later Silver transformation.

---

## 5.6 Orders

Table:

```text
orders
```

Summary:

- Rows: 99,441
- Columns: 8
- Distinct `order_id`: 99,441
- Distinct `customer_id`: 99,441
- Distinct `order_status`: 8

Important columns:

```text
order_id
customer_id
order_status
order_purchase_timestamp
order_approved_at
order_delivered_carrier_date
order_delivered_customer_date
order_estimated_delivery_date
```

Null values:

| Column | Nulls | Percentage |
|---|---:|---:|
| `order_approved_at` | 160 | 0.16% |
| `order_delivered_carrier_date` | 1,783 | 1.79% |
| `order_delivered_customer_date` | 2,965 | 2.98% |

Interpretation:

Nulls in approval and delivery dates are expected for canceled, unavailable, or incomplete orders. These records should not be removed in the Bronze layer.

Decision:

- Keep all orders in Bronze.
- Create logistics completeness flags in Silver.
- Use `order_status` to define business rules for Gold-level metrics.

---

## 5.7 Products

Table:

```text
products
```

Summary:

- Rows: 32,951
- Columns: 9
- Distinct `product_id`: 32,951
- Distinct `product_category_name`: 74

Important columns:

```text
product_id
product_category_name
product_name_lenght
product_description_lenght
product_photos_qty
product_weight_g
product_length_cm
product_height_cm
product_width_cm
```

> Note: The original dataset uses the column name `lenght` instead of `length` for some product metadata fields. This should be handled carefully during data cleaning without losing traceability to the raw schema.

Null values:

| Column | Nulls | Percentage |
|---|---:|---:|
| `product_category_name` | 610 | 1.85% |
| `product_name_lenght` | 610 | 1.85% |
| `product_description_lenght` | 610 | 1.85% |
| `product_photos_qty` | 610 | 1.85% |
| `product_weight_g` | 2 | 0.01% |
| `product_length_cm` | 2 | 0.01% |
| `product_height_cm` | 2 | 0.01% |
| `product_width_cm` | 2 | 0.01% |

Interpretation:

A small percentage of products have incomplete metadata. This should be handled explicitly in the Silver layer instead of silently dropping records.

Decision:

- Replace missing product categories with `unknown`.
- Create data quality flags.

Suggested flags:

```text
has_missing_category
has_missing_product_metadata
has_missing_dimensions
```

---

## 5.8 Sellers

Table:

```text
sellers
```

Summary:

- Rows: 3,095
- Columns: 4
- Distinct `seller_id`: 3,095
- Null values: none detected

Important columns:

```text
seller_id
seller_zip_code_prefix
seller_city
seller_state
```

Interpretation:

This table is clean and can be used to build seller dimensions and seller performance metrics.

Decision:

Use this table as the base for:

```text
dim_sellers
seller_performance
```

---

## 5.9 Category Translation

Table:

```text
category_translation
```

Summary:

- Rows: 71
- Columns: 2
- Null values: none detected
- `product_category_name` appears unique.
- `product_category_name_english` appears unique.

Important columns:

```text
product_category_name
product_category_name_english
```

Interpretation:

This is a clean auxiliary table that can enrich the product dimension.

Decision:

Join this table with `products` in the Silver or Gold layer to create English product category labels.

---

## 6. Relationship Checks

Relationship validation results:

| Check | Result |
|---|---:|
| Orders without customer | 0 |
| Customers without orders | 0 |
| Orders without items | 775 |
| Orders without payments | 1 |
| Orders without reviews | 768 |
| Order items without product | 0 |
| Order items without seller | 0 |

Interpretation:

The main entity relationships are mostly consistent:

- Every order has a customer.
- Every customer has at least one order.
- Every order item has a valid product.
- Every order item has a valid seller.

However, there are some incomplete business records:

- Some orders do not have items.
- One order does not have payment information.
- Some orders do not have reviews.

Decision:

- Do not delete incomplete records in Bronze.
- Investigate incomplete records in Silver.
- Exclude or flag records depending on the Gold-layer business metric.

---

## 7. Temporal Profile

## 7.1 Orders

| Column | Minimum Date | Maximum Date |
|---|---|---|
| `order_purchase_timestamp` | 2016-09-04 21:15:19 | 2018-10-17 17:30:18 |
| `order_approved_at` | 2016-09-15 12:16:38 | 2018-09-03 17:40:06 |
| `order_delivered_carrier_date` | 2016-10-08 10:34:01 | 2018-09-11 19:48:28 |
| `order_delivered_customer_date` | 2016-10-11 13:46:32 | 2018-10-17 13:22:46 |
| `order_estimated_delivery_date` | 2016-09-30 00:00:00 | 2018-11-12 00:00:00 |

Interpretation:

The order data mainly covers the period from September 2016 to October 2018.

---

## 7.2 Order Items

| Column | Minimum Date | Maximum Date |
|---|---|---|
| `shipping_limit_date` | 2016-09-19 00:15:34 | 2020-04-09 22:35:08 |

Interpretation:

The maximum `shipping_limit_date` is suspicious because it extends to 2020, while order purchases end in 2018.

Decision:

Create a temporal anomaly flag in Silver.

Suggested validation:

```text
shipping_limit_date should be close to order_purchase_timestamp
```

---

## 7.3 Reviews

| Column | Minimum Date | Maximum Date |
|---|---|---|
| `review_creation_date` | 2016-10-02 00:00:00 | 2018-08-31 00:00:00 |
| `review_answer_timestamp` | 2016-10-07 18:32:28 | 2018-10-29 12:27:35 |

Interpretation:

Review activity is consistent with the general lifecycle of the orders.

---

## 8. Data Quality Issues Detected

The main data quality issues detected are:

1. Reviews have a high percentage of missing text comments.
2. Some products have missing categories and metadata.
3. Some orders do not have associated order items.
4. One order does not have payment information.
5. Some orders do not have reviews.
6. `shipping_limit_date` contains a potential temporal outlier.
7. `customer_id` can be misleading if used for customer-level analytics.
8. Geolocation data has multiple records per zip code prefix and requires aggregation.

---

## 9. Initial Technical Decisions

## 9.1 Bronze Layer

Bronze should preserve the raw data as closely as possible to the source.

Rules:

- Do not delete rows.
- Do not apply business filters.
- Do not fix nulls.
- Add technical metadata columns.
- Store data in columnar format.

Suggested metadata columns:

```text
_ingestion_timestamp
_source_file
_layer
```

Initial storage format:

```text
Parquet
```

Future storage format:

```text
Delta Lake
```

---

## 9.2 Silver Layer

Silver should contain cleaned, standardized, and validated data.

Planned transformations:

- Standardize column names if needed.
- Ensure correct data types.
- Create data quality flags.
- Aggregate payments by `order_id`.
- Aggregate geolocation by zip code prefix.
- Handle missing product categories.
- Create logistics-related flags.
- Detect temporal anomalies.
- Preserve traceability to raw records.

---

## 9.3 Gold Layer

Gold should contain analytics-ready tables.

Planned tables:

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

Gold tables should support:

- Business intelligence dashboards
- Customer analytics
- Sales analytics
- Seller performance analysis
- Delivery performance analysis
- Machine learning feature generation

---

## 10. Implications for the Analytical Base Table

The main Analytical Base Table should be built at this grain:

```text
customer_unique_id
```

Candidate features:

```text
customer_unique_id
total_orders
total_items
total_spent
avg_order_value
avg_review_score
recency_days
frequency
monetary_value
avg_delivery_delay_days
late_delivery_rate
most_used_payment_type
favorite_category
customer_state
first_purchase_date
last_purchase_date
```

Potential target variable:

```text
churn_flag
```

Possible churn definition:

```text
A customer is considered churned if they have not purchased again within a defined observation window.
```

This definition must be refined later depending on the modeling window and business assumptions.

---

## 11. Recommended Next Steps

The next step is to build the Bronze ingestion job.

Target script:

```text
spark_jobs/bronze/load_raw_to_bronze.py
```

The Bronze job should:

1. Read all raw CSV files from `data/raw/olist/`.
2. Add technical metadata columns.
3. Write each table to `data/bronze/`.
4. Store outputs in Parquet format.
5. Preserve the raw structure without business transformations.

Expected Bronze output:

```text
data/bronze/customers/
data/bronze/geolocation/
data/bronze/order_items/
data/bronze/payments/
data/bronze/reviews/
data/bronze/orders/
data/bronze/products/
data/bronze/sellers/
data/bronze/category_translation/
```

---

## 12. Conclusion

The Olist dataset is suitable for an end-to-end Data Engineering project because it contains:

- Multiple relational entities
- Transactional data
- Customer behavior data
- Product and seller data
- Payment data
- Review data
- Geolocation data
- Realistic data quality issues
- Temporal attributes
- Good opportunities for batch and simulated streaming processing

This dataset can support a complete portfolio project involving:

```text
Python
PySpark
Data Lake
Medallion Architecture
Parquet
Delta Lake
Airflow
dbt
Kafka
PostgreSQL
Power BI
Data Quality
Analytical Base Tables
```

The profiling phase confirms that the dataset is complex enough to demonstrate realistic Data Engineering skills and should be used as the main source for the project.
