# Gold Layer Validation

## 1. Objective

This document validates that the **Gold layer** was successfully built from the previously validated Silver layer.

The Gold layer represents the analytics-ready layer of the Data Lake. It is designed to provide clean, modeled, and business-oriented datasets that can be consumed by dashboards, analytical queries, downstream data marts, and future machine learning feature engineering processes.

Unlike the Silver layer, which focuses on cleaning, standardization, and data quality, the Gold layer focuses on **business modeling**.

---

## 2. Input and Output Paths

### Silver input path

```text
data/silver/
```

### Gold output path

```text
data/gold/
```

Expected Gold datasets:

```text
data/gold/dim_customers/
data/gold/dim_products/
data/gold/dim_sellers/
data/gold/dim_dates/
data/gold/fact_orders/
data/gold/fact_order_items/
data/gold/fact_payments/
data/gold/fact_reviews/
data/gold/mart_sales_by_month/
```

---

## 3. Gold Layer Scope

The Gold layer was built to provide analytics-ready datasets based on dimensional modeling principles.

Main Gold objectives:

1. Build dimension tables for descriptive business entities.
2. Build fact tables for measurable business events.
3. Create an initial analytical mart for monthly sales analysis.
4. Preserve processing metadata for traceability.
5. Prepare the project for BI dashboards and future ABT generation.

---

## 4. Gold Tables Generated

## 4.1 `dim_customers`

Gold table:

```text
data/gold/dim_customers/
```

Grain:

```text
One row per customer_unique_id
```

Purpose:

This dimension represents unique customers and their main customer attributes.

Main fields:

```text
customer_unique_id
sample_customer_id
customer_zipcode_prefix
customer_city
customer_state
total_orders_registered
```

Notes:

- `customer_unique_id` is used as the true customer-level identifier.
- `customer_id` is treated as an order/customer relationship identifier.
- The dimension includes the number of registered orders per unique customer.

---

## 4.2 `dim_products`

Gold table:

```text
data/gold/dim_products/
```

Grain:

```text
One row per product_id
```

Purpose:

This dimension represents product attributes and product quality indicators.

Main fields:

```text
product_id
product_category_name
product_category_name_english
product_name_length
product_description_length
product_photos_qty
product_weight_g
product_length_cm
product_height_cm
product_width_cm
has_missing_category
has_missing_product_metadata
has_missing_dimensions
```

Notes:

- Product category translations are included.
- Missing product category and metadata issues are preserved as flags.

---

## 4.3 `dim_sellers`

Gold table:

```text
data/gold/dim_sellers/
```

Grain:

```text
One row per seller_id
```

Purpose:

This dimension represents sellers and their location attributes.

Main fields:

```text
seller_id
seller_zipcode_prefix
seller_city
seller_state
```

Notes:

This table will support seller performance analysis and seller-level business reporting.

---

## 4.4 `dim_dates`

Gold table:

```text
data/gold/dim_dates/
```

Grain:

```text
One row per order_purchase_date
```

Purpose:

This dimension supports time-based analysis for orders, sales, payments, and dashboards.

Main fields:

```text
date
year
quarter
month
day
day_of_week
```

Validation result:

```text
Actual rows: 634
```

Notes:

The date dimension was generated from distinct order purchase dates.

---

## 4.5 `fact_orders`

Gold table:

```text
data/gold/fact_orders/
```

Grain:

```text
One row per order_id
```

Purpose:

This fact table represents order-level business events enriched with customer, payment, review, and delivery attributes.

Main fields:

```text
order_id
customer_id
customer_unique_id
order_status
order_purchase_timestamp
order_purchase_date
order_approved_at
order_delivered_carrier_date
order_delivered_customer_date
order_estimated_delivery_date
is_delivered
is_approved
has_carrier_delivery_date
has_customer_delivery_date
delivery_delay_days
is_late_delivery
has_missing_logistics_dates
total_payment_value
payment_count
max_payment_installments
main_payment_type
has_multiple_payments
review_score
has_review_comment
```

Business purpose:

This table supports order-level analytics, delivery analysis, customer satisfaction analysis, and payment behavior analysis.

---

## 4.6 `fact_order_items`

Gold table:

```text
data/gold/fact_order_items/
```

Grain:

```text
One row per order_id and order_item_id
```

Purpose:

This fact table represents item-level sales events.

Main fields:

```text
order_id
order_item_id
customer_id
product_id
seller_id
order_purchase_date
order_status
product_category_name_english
seller_state
shipping_limit_date
price
freight_value
item_total_value
has_negative_price
has_negative_freight
```

Business purpose:

This table supports product sales analysis, seller performance analysis, category-level revenue analysis, freight analysis, and item-level order analytics.

---

## 4.7 `fact_payments`

Gold table:

```text
data/gold/fact_payments/
```

Grain:

```text
One row per order_id
```

Purpose:

This fact table represents payment metrics aggregated at order level.

Main fields:

```text
order_id
total_payment_value
payment_count
max_payment_installments
main_payment_type
has_multiple_payments
```

Business purpose:

This table supports payment method analysis, installment analysis, and financial reporting.

---

## 4.8 `fact_reviews`

Gold table:

```text
data/gold/fact_reviews/
```

Grain:

```text
One row per order_id
```

Purpose:

This fact table represents customer review information at order level.

Main fields:

```text
review_id
order_id
review_score
has_review_comment
review_creation_date
review_answer_timestamp
```

Business purpose:

This table supports review score analysis, customer satisfaction reporting, and future customer experience features.

---

## 4.9 `mart_sales_by_month`

Gold table:

```text
data/gold/mart_sales_by_month/
```

Grain:

```text
One row per year and month
```

Purpose:

This analytical mart provides monthly sales metrics ready for BI consumption.

Main fields:

```text
year
month
total_order_items
total_product_revenue
total_freight_value
total_item_value
```

Validation result:

```text
Actual rows: 24
```

Business purpose:

This mart can be used directly for a Power BI dashboard showing monthly revenue, freight value, and order item volume.

---

## 5. Technical Metadata Validation

Each Gold dataset includes the following metadata columns:

```text
_gold_processed_timestamp
_layer
```

### Metadata purpose

| Column | Purpose |
|---|---|
| `_gold_processed_timestamp` | Captures when the record was processed into the Gold layer. |
| `_layer` | Identifies the current lakehouse layer as `gold`. |

---

## 6. Validation Script

The Gold validation was performed using the following script:

```text
spark_jobs/gold/validate_gold.py
```

The script validates:

1. Row counts per Gold table.
2. Presence of required Gold metadata columns.
3. Basic readability of each Gold Parquet dataset.

---

## 7. Validation Summary

| Table | Expected Rows | Actual Rows | Count Status | Metadata Status |
|---|---:|---:|---|---|
| `dim_customers` | 96,096 | 96,096 | OK | OK |
| `dim_products` | 32,951 | 32,951 | OK | OK |
| `dim_sellers` | 3,095 | 3,095 | OK | OK |
| `dim_dates` | At least 1 | 634 | OK | OK |
| `fact_orders` | 99,441 | 99,441 | OK | OK |
| `fact_order_items` | 112,650 | 112,650 | OK | OK |
| `fact_payments` | 99,440 | 99,440 | OK | OK |
| `fact_reviews` | 98,673 | 98,673 | OK | OK |
| `mart_sales_by_month` | At least 1 | 24 | OK | OK |

---

## 8. Result

All Gold datasets passed validation successfully.

```text
dim_customers: count_status=OK, metadata_status=OK
dim_products: count_status=OK, metadata_status=OK
dim_sellers: count_status=OK, metadata_status=OK
dim_dates: count_status=OK, metadata_status=OK
fact_orders: count_status=OK, metadata_status=OK
fact_order_items: count_status=OK, metadata_status=OK
fact_payments: count_status=OK, metadata_status=OK
fact_reviews: count_status=OK, metadata_status=OK
mart_sales_by_month: count_status=OK, metadata_status=OK
```

Final validation status:

```text
Gold validation completed successfully.
```

---

## 9. Gold Layer Output

The Gold layer generated the following analytics-ready datasets:

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
```

These datasets are ready for:

- BI dashboards
- Analytical SQL queries
- Data marts
- Customer ABT generation
- Future warehouse loading

---

## 10. Data Pipeline Status

The project has now completed the following pipeline stages:

```text
Raw CSV files
    ↓
Bronze Parquet datasets
    ↓
Silver curated Parquet datasets
    ↓
Gold analytics-ready datasets
```

---

## 11. Conclusion

The Gold layer was successfully built and validated.

The project now contains a functional lakehouse-style analytical pipeline with dimensional tables, fact tables, and an initial sales mart.

The Gold layer introduced business-oriented modeling and prepared the project for downstream analytical use cases.

The next recommended phase is:

```text
Customer ABT — Analytical Base Table for customer-level analytics and ML-ready features
```

Planned Customer ABT features:

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
churn_flag
```
