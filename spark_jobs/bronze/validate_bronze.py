from pyspark.sql import SparkSession


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("Validate Bronze Layer")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    spark = create_spark_session()

    bronze_base_path = "data/bronze"

    expected_counts = {
        "customers": 99441,
        "geolocation": 1000163,
        "order_items": 112650,
        "payments": 103886,
        "reviews": 99224,
        "orders": 99441,
        "products": 32951,
        "sellers": 3095,
        "category_translation": 71,
    }

    validation_results = []

    for table_name, expected_count in expected_counts.items():
        table_path = f"{bronze_base_path}/{table_name}"

        print("=" * 80)
        print(f"Validating Bronze table: {table_name}")
        print("=" * 80)

        df = spark.read.parquet(table_path)

        actual_count = df.count()
        columns = df.columns

        has_ingestion_timestamp = "_ingestion_timestamp" in columns
        has_source_file = "_source_file" in columns
        has_source_table = "_source_table" in columns
        has_layer = "_layer" in columns

        count_status = "OK" if actual_count == expected_count else "FAILED"

        metadata_status = (
            "OK"
            if all(
                [
                    has_ingestion_timestamp,
                    has_source_file,
                    has_source_table,
                    has_layer,
                ]
            )
            else "FAILED"
        )

        print(f"Expected rows: {expected_count}")
        print(f"Actual rows:   {actual_count}")
        print(f"Count status:  {count_status}")
        print(f"Metadata status: {metadata_status}")
        print("Columns:")
        print(columns)

        if metadata_status == "OK":
            print("Metadata sample:")
            df.select(
                "_ingestion_timestamp",
                "_source_file",
                "_source_table",
                "_layer",
            ).show(3, truncate=False)

        validation_results.append(
            {
                "table_name": table_name,
                "expected_count": expected_count,
                "actual_count": actual_count,
                "count_status": count_status,
                "metadata_status": metadata_status,
            }
        )

    print("\n" + "=" * 80)
    print("BRONZE VALIDATION SUMMARY")
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
        result
        for result in validation_results
        if result["count_status"] != "OK" or result["metadata_status"] != "OK"
    ]

    print("\n" + "=" * 80)
    print("FINAL STATUS")
    print("=" * 80)

    if len(failed_validations) == 0:
        print("Bronze validation completed successfully.")
    else:
        print("Bronze validation failed for the following tables:")

        for result in failed_validations:
            print(
                f"- {result['table_name']}: "
                f"count_status={result['count_status']}, "
                f"metadata_status={result['metadata_status']}"
            )

    spark.stop()


if __name__ == "__main__":
    main()