# Bronze Layer Validation

## 1. Objective

This document validates that all raw Olist CSV files were successfully ingested into the **Bronze layer** as Parquet datasets.

The Bronze layer is designed to preserve the raw source data as closely as possible while adding technical metadata required for traceability.

---

## 2. Input and Output Paths

### Raw input path

```text
data/raw/olist/
```

### Bronze output path

```text
data/bronze/
```

Expected Bronze datasets:

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

## 3. Validation Summary

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

## 4. Technical Metadata Validation

Each Bronze dataset includes the following metadata columns:

```text
_ingestion_timestamp
_source_file
_source_table
_layer
```

### Metadata purpose

| Column | Purpose |
|---|---|
| `_ingestion_timestamp` | Captures when the record was ingested into the Bronze layer. |
| `_source_file` | Stores the original source file path used by Spark. |
| `_source_table` | Identifies the logical source table name. |
| `_layer` | Identifies the current lakehouse layer as `bronze`. |

---

## 5. Validation Script

The validation was performed using the following script:

```text
spark_jobs/bronze/validate_bronze.py
```

The script validates:

1. Row counts per Bronze table.
2. Presence of required technical metadata columns.
3. Basic readability of each Parquet dataset.

---

## 6. Result

All Bronze datasets passed validation successfully.

```text
customers: count_status=OK, metadata_status=OK
geolocation: count_status=OK, metadata_status=OK
order_items: count_status=OK, metadata_status=OK
payments: count_status=OK, metadata_status=OK
reviews: count_status=OK, metadata_status=OK
orders: count_status=OK, metadata_status=OK
products: count_status=OK, metadata_status=OK
sellers: count_status=OK, metadata_status=OK
category_translation: count_status=OK, metadata_status=OK
```

---

## 7. Conclusion

The Bronze layer was successfully created and validated.

All row counts match the expected values from the raw data profiling phase, and all required technical metadata columns are present in every Bronze dataset.

The pipeline has successfully completed the following flow:

```text
Raw CSV files
    ↓
PySpark ingestion
    ↓
Technical metadata enrichment
    ↓
Parquet persistence
    ↓
Bronze validation
```

The project is ready to move into the next phase:

```text
Silver Layer — data cleaning, standardization, validation, and business-level transformations
```
