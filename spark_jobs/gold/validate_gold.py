from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("Validate Gold Layer")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    spark = create_spark_session()
    gold_base_path = "data/gold"

    validations = {
        "dim_customers": {"expected_count": 96096, "mode": "equals"},
        "dim_products": {"expected_count": 32951, "mode": "equals"},
        "dim_sellers": {"expected_count": 3095, "mode": "equals"},
        "dim_dates": {"expected_count": 1, "mode": "at_least"},
        "fact_orders": {"expected_count": 99441, "mode": "equals"},
        "fact_order_items": {"expected_count": 112650, "mode": "equals"},
        "fact_payments": {"expected_count": 99440, "mode": "equals"},
        "fact_reviews": {"expected_count": 98673, "mode": "equals"},
        "mart_sales_by_month": {"expected_count": 1, "mode": "at_least"},
    }

    validation_results = []

    for table_name, rule in validations.items():
        table_path = f"{gold_base_path}/{table_name}"
        expected_count = rule["expected_count"]
        mode = rule["mode"]

        print("=" * 80)
        print(f"Validating Gold table: {table_name}")
        print("=" * 80)

        df = spark.read.parquet(table_path)
        actual_count = df.count()
        columns = df.columns

        has_gold_processed_timestamp = "_gold_processed_timestamp" in columns
        has_layer = "_layer" in columns

        if mode == "equals":
            count_status = "OK" if actual_count == expected_count else "FAILED"
            expectation_text = f"expected={expected_count}"
        else:
            count_status = "OK" if actual_count >= expected_count else "FAILED"
            expectation_text = f"expected_at_least={expected_count}"

        metadata_status = "OK" if has_gold_processed_timestamp and has_layer else "FAILED"

        print(f"Expectation:    {expectation_text}")
        print(f"Actual rows:    {actual_count}")
        print(f"Count status:   {count_status}")
        print(f"Metadata status:{metadata_status}")
        print("Columns:")
        print(columns)

        validation_results.append({
            "table_name": table_name,
            "expectation": expectation_text,
            "actual_count": actual_count,
            "count_status": count_status,
            "metadata_status": metadata_status,
        })

    print("\n" + "=" * 80)
    print("GOLD VALIDATION SUMMARY")
    print("=" * 80)

    for result in validation_results:
        print(
            f"{result['table_name']}: "
            f"{result['expectation']}, "
            f"actual={result['actual_count']}, "
            f"count_status={result['count_status']}, "
            f"metadata_status={result['metadata_status']}"
        )

    failed_validations = [
        result for result in validation_results
        if result["count_status"] != "OK" or result["metadata_status"] != "OK"
    ]

    print("\n" + "=" * 80)
    print("FINAL STATUS")
    print("=" * 80)

    if len(failed_validations) == 0:
        print("Gold validation completed successfully.")
    else:
        print("Gold validation failed for the following tables:")
        for result in failed_validations:
            print(
                f"- {result['table_name']}: "
                f"count_status={result['count_status']}, "
                f"metadata_status={result['metadata_status']}"
            )

    spark.stop()


if __name__ == "__main__":
    main()
