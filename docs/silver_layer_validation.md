# Silver Layer Validation

## 1. Objective

This document validates that the **Silver layer** was successfully built from the previously validated Bronze layer.

The Silver layer is responsible for transforming raw-but-structured Bronze datasets into cleaned, standardized, and business-ready datasets. Unlike the Bronze layer, which preserves the source data as closely as possible, the Silver layer applies data cleaning, deduplication, standardization, aggregations, and data quality flags.

---

## 2. Input and Output Paths

### Bronze input path

```text
data/bronze/
```

### Silver output path

```text
data/silver/
```

Expected Silver datasets:

```text
data/silver/customers/
data/silver/orders/
data/silver/order_items/
data/silver/payments/
data/silver/reviews/
data/silver/products/
data/silver/sellers/
data/silver/geolocation_zipcode/
```

---

## 3. Silver Layer Scope

The Silver layer was built to prepare the Olist e-commerce data for analytical modeling, Gold layer construction, and future ABT generation.

Main Silver objectives:

1. Clean and standardize Bronze datasets.
2. Deduplicate records where needed.
3. Add business and data quality flags.
4. Aggregate datasets that should not be consumed at raw grain.
5. Preserve traceability through Silver metadata columns.
6. Prepare data for Gold dimensional modeling.

---

## 4. Silver Transformations by Dataset

## 4.1 Customers

Silver table:

```text
data/silver/customers/
```

Transformations applied:

- Selected relevant customer columns.
- Renamed `customer_zip_code_prefix` to `customer_zipcode_prefix`.
- Standardized `customer_city` using lowercase and trim operations.
- Trimmed `customer_state`.
- Deduplicated records by `customer_id`.

Business purpose:

This table will support future customer dimensions and customer-level ABT construction. For behavioral analytics, the key customer identifier remains `customer_unique_id`.

---

## 4.2 Orders

Silver table:

```text
data/silver/orders/
```

Transformations applied:

- Selected relevant order columns.
- Created `order_purchase_date` from `order_purchase_timestamp`.
- Created delivery and logistics flags:

```text
is_delivered
is_approved
has_carrier_delivery_date
has_customer_delivery_date
has_missing_logistics_dates
```

- Created delivery performance fields:

```text
delivery_delay_days
is_late_delivery
```

- Deduplicated records by `order_id`.

Business purpose:

This table will support order-level analytics, delivery analysis, late delivery metrics, and Gold fact table construction.

---

## 4.3 Order Items

Silver table:

```text
data/silver/order_items/
```

Transformations applied:

- Selected relevant order item columns.
- Created `item_total_value` as:

```text
price + freight_value
```

- Created quality flags:

```text
has_negative_price
has_negative_freight
```

- Deduplicated records by:

```text
order_id, order_item_id
```

Business purpose:

This table will serve as the base for product sales, seller performance, and order item fact tables.

---

## 4.4 Payments

Silver table:

```text
data/silver/payments/
```

Transformations applied:

- Aggregated payments at `order_id` level.
- Created payment metrics:

```text
total_payment_value
payment_count
max_payment_installments
main_payment_type
has_multiple_payments
```

- Selected the main payment type based on the highest payment value per order.

Business purpose:

The original payments table can contain multiple rows per order. The Silver payments table creates one row per order, making it easier and safer to join with orders and build financial metrics.

---

## 4.5 Reviews

Silver table:

```text
data/silver/reviews/
```

Transformations applied:

- Ranked reviews by `order_id` using the latest `review_answer_timestamp` and `review_creation_date`.
- Kept one review record per `order_id`.
- Created `has_review_comment` flag.
- Deduplicated reviews at order level.

Business purpose:

This table will support customer satisfaction analysis through `review_score`, while preserving a flag that identifies whether textual feedback exists.

---

## 4.6 Products

Silver table:

```text
data/silver/products/
```

Transformations applied:

- Renamed misspelled source columns:

```text
product_name_lenght → product_name_length
product_description_lenght → product_description_length
```

- Replaced missing product categories with:

```text
unknown
```

- Joined product categories with the category translation table.
- Created English product category labels.
- Created product quality flags:

```text
has_missing_category
has_missing_product_metadata
has_missing_dimensions
```

- Deduplicated records by `product_id`.

Business purpose:

This table will support product dimensions, product category analysis, and product quality checks.

---

## 4.7 Sellers

Silver table:

```text
data/silver/sellers/
```

Transformations applied:

- Selected relevant seller columns.
- Renamed `seller_zip_code_prefix` to `seller_zipcode_prefix`.
- Standardized `seller_city` using lowercase and trim operations.
- Trimmed `seller_state`.
- Deduplicated records by `seller_id`.

Business purpose:

This table will support seller dimensions and seller performance analytics.

---

## 4.8 Geolocation Zipcode

Silver table:

```text
data/silver/geolocation_zipcode/
```

Transformations applied:

- Aggregated raw geolocation records by `geolocation_zip_code_prefix`.
- Created one row per zip code prefix.
- Calculated:

```text
avg_lat
avg_lng
city
state
geolocation_record_count
```

- Renamed `geolocation_zip_code_prefix` to `zipcode_prefix`.

Business purpose:

The original geolocation table contains multiple records per zip code prefix. This aggregated Silver table makes geolocation usable as a lookup table for customers and sellers.

---

## 5. Technical Metadata Validation

Each Silver dataset includes the following metadata columns:

```text
_silver_processed_timestamp
_layer
```

### Metadata purpose

| Column | Purpose |
|---|---|
| `_silver_processed_timestamp` | Captures when the record was processed into the Silver layer. |
| `_layer` | Identifies the current lakehouse layer as `silver`. |

---

## 6. Validation Script

The Silver validation was performed using the following script:

```text
spark_jobs/silver/validate_silver.py
```

The script validates:

1. Row counts per Silver table.
2. Presence of required Silver metadata columns.
3. Basic readability of each Silver Parquet dataset.

---

## 7. Validation Summary

| Table | Expected Rows | Actual Rows | Count Status | Metadata Status |
|---|---:|---:|---|---|
| `customers` | 99,441 | 99,441 | OK | OK |
| `orders` | 99,441 | 99,441 | OK | OK |
| `order_items` | 112,650 | 112,650 | OK | OK |
| `payments` | 99,440 | 99,440 | OK | OK |
| `reviews` | 98,673 | 98,673 | OK | OK |
| `products` | 32,951 | 32,951 | OK | OK |
| `sellers` | 3,095 | 3,095 | OK | OK |
| `geolocation_zipcode` | 19,015 | 19,015 | OK | OK |

---

## 8. Result

All Silver datasets passed validation successfully.

```text
customers: count_status=OK, metadata_status=OK
orders: count_status=OK, metadata_status=OK
order_items: count_status=OK, metadata_status=OK
payments: count_status=OK, metadata_status=OK
reviews: count_status=OK, metadata_status=OK
products: count_status=OK, metadata_status=OK
sellers: count_status=OK, metadata_status=OK
geolocation_zipcode: count_status=OK, metadata_status=OK
```

Final validation status:

```text
Silver validation completed successfully.
```

---

## 9. Silver Layer Output

The Silver layer generated the following curated datasets:

```text
customers
orders
order_items
payments
reviews
products
sellers
geolocation_zipcode
```

These datasets are now ready to be used for Gold layer modeling.

---

## 10. Conclusion

The Silver layer was successfully built and validated.

The pipeline has now completed the following flow:

```text
Raw CSV files
    ↓
Bronze Parquet datasets
    ↓
Silver curated Parquet datasets
```

The Silver layer introduced meaningful data engineering transformations, including cleaning, standardization, deduplication, aggregation, and data quality flag creation.

The project is ready to move into the next phase:

```text
Gold Layer — dimensional modeling and analytics-ready tables
```

Planned Gold outputs:

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
