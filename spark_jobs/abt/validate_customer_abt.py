from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, sum as spark_sum


EXPECTED_CUSTOMER_COUNT = 96096
REQUIRED_COLUMNS = [
    "customer_unique_id",
    "customer_state",
    "customer_city",
    "customer_zipcode_prefix",
    "total_orders",
    "total_items",
    "total_spent",
    "avg_order_value",
    "avg_review_score",
    "recency_days",
    "frequency",
    "monetary_value",
    "late_delivery_rate",
    "avg_delivery_delay_days",
    "most_used_payment_type",
    "favorite_category",
    "first_purchase_date",
    "last_purchase_date",
    "churn_flag",
    "_abt_processed_timestamp",
    "_layer",
]


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("Validate Customer ABT")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    spark = create_spark_session()

    abt_path = "data/gold/customer_abt"
    customer_abt = spark.read.parquet(abt_path)

    row_count = customer_abt.count()
    distinct_customer_count = customer_abt.select("customer_unique_id").distinct().count()
    missing_required_columns = [column for column in REQUIRED_COLUMNS if column not in customer_abt.columns]

    count_status = "OK" if row_count == EXPECTED_CUSTOMER_COUNT else "FAILED"
    uniqueness_status = "OK" if row_count == distinct_customer_count else "FAILED"
    required_columns_status = "OK" if len(missing_required_columns) == 0 else "FAILED"

    null_customer_id_count = customer_abt.filter(col("customer_unique_id").isNull()).count()
    null_customer_status = "OK" if null_customer_id_count == 0 else "FAILED"

    invalid_churn_count = customer_abt.filter(~col("churn_flag").isin([0, 1])).count()
    churn_status = "OK" if invalid_churn_count == 0 else "FAILED"

    print("=" * 80)
    print("CUSTOMER ABT VALIDATION SUMMARY")
    print("=" * 80)
    print(f"Expected rows: {EXPECTED_CUSTOMER_COUNT}")
    print(f"Actual rows:   {row_count}")
    print(f"Distinct customer_unique_id: {distinct_customer_count}")
    print(f"Row count status: {count_status}")
    print(f"Uniqueness status: {uniqueness_status}")
    print(f"Required columns status: {required_columns_status}")
    print(f"Null customer_unique_id status: {null_customer_status}")
    print(f"Churn flag status: {churn_status}")

    if missing_required_columns:
        print("Missing required columns:")
        for column in missing_required_columns:
            print(f"- {column}")

    print("\nCustomer ABT sample:")
    customer_abt.select(
        "customer_unique_id",
        "customer_state",
        "total_orders",
        "total_items",
        "total_spent",
        "recency_days",
        "churn_flag",
        "most_used_payment_type",
        "favorite_category",
    ).show(10, truncate=False)

    print("\nChurn distribution:")
    customer_abt.groupBy("churn_flag").agg(count("customer_unique_id").alias("customers")).show()

    print("\n" + "=" * 80)
    print("FINAL STATUS")
    print("=" * 80)

    statuses = [
        count_status,
        uniqueness_status,
        required_columns_status,
        null_customer_status,
        churn_status,
    ]

    if all(status == "OK" for status in statuses):
        print("Customer ABT validation completed successfully.")
    else:
        print("Customer ABT validation failed.")

    spark.stop()


if __name__ == "__main__":
    main()
