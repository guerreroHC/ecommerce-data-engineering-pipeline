from pyspark.sql import SparkSession


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("Validate Silver Layer")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    spark = create_spark_session()
    silver_base_path = "data/silver"

    expected_counts = {
        "customers": 99441,
        "orders": 99441,
        "order_items": 112650,
        "payments": 99440,
        "reviews": 98673,
        "products": 32951,
        "sellers": 3095,
        "geolocation_zipcode": 19015,
    }

    validation_results = []

    for table_name, expected_count in expected_counts.items():
        table_path = f"{silver_base_path}/{table_name}"

        print("=" * 80)
        print(f"Validating Silver table: {table_name}")
        print("=" * 80)

        df = spark.read.parquet(table_path)
        actual_count = df.count()
        columns = df.columns

        has_silver_processed_timestamp = "_silver_processed_timestamp" in columns
        has_layer = "_layer" in columns

        count_status = "OK" if actual_count == expected_count else "FAILED"
        metadata_status = "OK" if has_silver_processed_timestamp and has_layer else "FAILED"

        print(f"Expected rows: {expected_count}")
        print(f"Actual rows:   {actual_count}")
        print(f"Count status:  {count_status}")
        print(f"Metadata status: {metadata_status}")
        print("Columns:")
        print(columns)

        validation_results.append({
            "table_name": table_name,
            "expected_count": expected_count,
            "actual_count": actual_count,
            "count_status": count_status,
            "metadata_status": metadata_status,
        })

    print("" + "=" * 80)
    print("SILVER VALIDATION SUMMARY")
    print("=" * 80)

    for result in validation_results:
        print(
            f"{result['table_name']}: "
            f"expected={result['expected_count']}, "
            f"actual={result['actual_count']}, "
            f"count_status={result['count_status']}, "
            f"metadata_status={result['metadata_status']}"
        )

    failed_validations = [
        result for result in validation_results
        if result["count_status"] != "OK" or result["metadata_status"] != "OK"
    ]

    print("" + "=" * 80)
    print("FINAL STATUS")
    print("=" * 80)

    if len(failed_validations) == 0:
        print("Silver validation completed successfully.")
    else:
        print("Silver validation failed for the following tables:")
        for result in failed_validations:
            print(
                f"- {result['table_name']}: "
                f"count_status={result['count_status']}, "
                f"metadata_status={result['metadata_status']}"
            )

    spark.stop()


if __name__ == "__main__":
    main()
