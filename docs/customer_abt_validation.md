# Customer ABT Validation

## 1. Objective

This document validates that the **Customer Analytical Base Table (Customer ABT)** was successfully built from the Gold layer.

The Customer ABT is designed as a customer-level analytical dataset that consolidates behavioral, transactional, payment, delivery, review, and product-category features into one table.

The main purpose of this ABT is to support:

- Customer analytics
- Churn analysis
- Segmentation
- Feature engineering
- Machine learning preparation
- BI-ready customer-level reporting

---

## 2. Input and Output Paths

### Gold input datasets

```text
data/gold/dim_customers/
data/gold/fact_orders/
data/gold/fact_order_items/
```

### Customer ABT output path

```text
data/gold/customer_abt/
```

---

## 3. ABT Grain

The Customer ABT was built at the following grain:

```text
One row per customer_unique_id
```

This means each row represents one unique customer.

The selected customer identifier is:

```text
customer_unique_id
```

This is the correct identifier for customer-level behavioral analytics because `customer_id` behaves as an order/customer relationship identifier in the Olist dataset, while `customer_unique_id` represents the same customer across multiple purchases.

---

## 4. ABT Features

The Customer ABT includes the following feature groups.

---

## 4.1 Customer Attributes

```text
customer_unique_id
customer_state
customer_city
customer_zipcode_prefix
```

Purpose:

These fields describe the customer's geographic attributes and serve as baseline segmentation variables.

---

## 4.2 Order Behavior Features

```text
total_orders
first_purchase_date
last_purchase_date
recency_days
frequency
```

Purpose:

These features describe customer purchase behavior over time.

Definitions:

- `total_orders`: total number of orders associated with the customer.
- `first_purchase_date`: first observed purchase date.
- `last_purchase_date`: most recent observed purchase date.
- `recency_days`: number of days between the global latest purchase date and the customer's last purchase date.
- `frequency`: number of purchases made by the customer.

---

## 4.3 Monetary Features

```text
total_spent
avg_order_value
monetary_value
total_product_revenue
total_freight_value
total_item_value
```

Purpose:

These features represent customer value and spending behavior.

Definitions:

- `total_spent`: total payment value associated with the customer.
- `avg_order_value`: average payment value per order.
- `monetary_value`: total monetary value used for RFM-style analysis.
- `total_product_revenue`: sum of product prices purchased by the customer.
- `total_freight_value`: sum of freight values associated with the customer.
- `total_item_value`: sum of product price plus freight value.

---

## 4.4 Product and Seller Behavior Features

```text
total_items
distinct_products_purchased
distinct_sellers_purchased_from
distinct_categories_purchased
favorite_category
```

Purpose:

These features describe the variety and type of products purchased by the customer.

Definitions:

- `total_items`: total number of order items purchased.
- `distinct_products_purchased`: number of distinct products purchased.
- `distinct_sellers_purchased_from`: number of distinct sellers purchased from.
- `distinct_categories_purchased`: number of distinct product categories purchased.
- `favorite_category`: most frequently purchased product category.

---

## 4.5 Payment Behavior Features

```text
most_used_payment_type
```

Purpose:

This field identifies the customer's most commonly used payment method.

---

## 4.6 Review and Satisfaction Features

```text
avg_review_score
```

Purpose:

This feature represents the customer's average review score across orders.

---

## 4.7 Delivery and Logistics Features

```text
late_delivery_rate
avg_delivery_delay_days
orders_with_missing_logistics_dates
```

Purpose:

These features describe the customer's delivery experience.

Definitions:

- `late_delivery_rate`: proportion of delivered orders that arrived after the estimated delivery date.
- `avg_delivery_delay_days`: average number of days between actual delivery and estimated delivery.
- `orders_with_missing_logistics_dates`: number of orders with missing logistics-related timestamps.

---

## 4.8 Target Variable

```text
churn_flag
```

Initial churn rule:

```text
churn_flag = 1 if recency_days > 180
churn_flag = 0 otherwise
```

Purpose:

This target variable provides a first simple churn definition for experimentation and feature validation.

Important note:

This churn definition is intentionally simple and should be refined later using a formal observation window and prediction window if the project evolves into a supervised machine learning use case.

---

## 5. Technical Metadata

The Customer ABT includes the following technical metadata columns:

```text
_abt_processed_timestamp
_layer
```

### Metadata purpose

| Column | Purpose |
|---|---|
| `_abt_processed_timestamp` | Captures when the record was processed into the Customer ABT. |
| `_layer` | Identifies the current lakehouse layer as `gold`. |

---

## 6. Validation Script

The Customer ABT validation was performed using the following script:

```text
spark_jobs/abt/validate_customer_abt.py
```

The script validates:

1. Expected row count.
2. Uniqueness of `customer_unique_id`.
3. Presence of required columns.
4. Absence of null `customer_unique_id` values.
5. Validity of `churn_flag` values.
6. Churn distribution.

---

## 7. Validation Summary

| Validation | Result |
|---|---|
| Expected rows | 96,096 |
| Actual rows | 96,096 |
| Distinct `customer_unique_id` | 96,096 |
| Row count status | OK |
| Uniqueness status | OK |
| Required columns status | OK |
| Null `customer_unique_id` status | OK |
| Churn flag status | OK |

---

## 8. Churn Distribution

| churn_flag | Customers |
|---:|---:|
| 1 | 68,204 |
| 0 | 27,892 |

Interpretation:

Using the initial churn definition of `recency_days > 180`, most customers are labeled as churned. This is expected in this dataset because many customers only purchased once and the dataset has a fixed historical observation period.

This distribution should be revisited later if a more rigorous ML-ready target definition is required.

---

## 9. Sample Output

The validation sample confirmed that the Customer ABT includes customer-level behavioral features such as:

```text
customer_unique_id
customer_state
total_orders
total_items
total_spent
recency_days
churn_flag
most_used_payment_type
favorite_category
```

Example feature values observed during validation:

```text
total_orders = 1
total_items = 1 or 2
total_spent = customer-specific monetary value
recency_days = number of days since last purchase
churn_flag = 0 or 1
most_used_payment_type = credit_card, boleto, etc.
favorite_category = telephony, sports_leisure, housewares, etc.
```

---

## 10. Result

The Customer ABT passed validation successfully.

```text
Customer ABT validation completed successfully.
```

---

## 11. Pipeline Status

The project has now completed the following pipeline stages:

```text
Raw CSV files
    ↓
Bronze Parquet datasets
    ↓
Silver curated Parquet datasets
    ↓
Gold analytics-ready datasets
    ↓
Customer Analytical Base Table
```

---

## 12. Conclusion

The Customer ABT was successfully built and validated.

The table contains one row per unique customer and includes meaningful customer-level features for analytics and future machine learning workflows.

This phase connects the Data Engineering pipeline with advanced analytics by producing a feature-rich analytical dataset.

The next recommended phases are:

```text
1. Load Gold and ABT tables into PostgreSQL
2. Create Power BI dashboards
3. Add Airflow orchestration
4. Refactor transformations with dbt
5. Add Kafka streaming simulation
6. Add data quality tests
```
