from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, input_file_name


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("Olist Bronze Ingestion")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_raw_csv(spark, file_path):
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("multiLine", True)
        .option("escape", '"')
        .csv(file_path)
    )

    return df


def add_bronze_metadata(df, table_name):
    df_with_metadata = (
        df
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", input_file_name())
        .withColumn("_source_table", lit(table_name))
        .withColumn("_layer", lit("bronze"))
    )

    return df_with_metadata


def write_bronze_table(df, output_path):
    (
        df.write
        .mode("overwrite")
        .parquet(output_path)
    )


def process_table(spark, table_name, input_path, output_path):
    print("=" * 80)
    print(f"Processing table: {table_name}")
    print("=" * 80)

    raw_df = read_raw_csv(spark, input_path)

    print(f"Raw rows for {table_name}: {raw_df.count()}")
    print("Raw schema:")
    raw_df.printSchema()

    bronze_df = add_bronze_metadata(raw_df, table_name)

    write_bronze_table(bronze_df, output_path)

    written_df = spark.read.parquet(output_path)
    written_count = written_df.count()

    print(f"Bronze rows written for {table_name}: {written_count}")
    print(f"Bronze output path: {output_path}")
    print()


def main():
    spark = create_spark_session()

    raw_base_path = "data/raw/olist"
    bronze_base_path = "data/bronze"

    tables = {
        "customers": {
            "input": f"{raw_base_path}/olist_customers_dataset.csv",
            "output": f"{bronze_base_path}/customers",
        },
        "geolocation": {
            "input": f"{raw_base_path}/olist_geolocation_dataset.csv",
            "output": f"{bronze_base_path}/geolocation",
        },
        "order_items": {
            "input": f"{raw_base_path}/olist_order_items_dataset.csv",
            "output": f"{bronze_base_path}/order_items",
        },
        "payments": {
            "input": f"{raw_base_path}/olist_order_payments_dataset.csv",
            "output": f"{bronze_base_path}/payments",
        },
        "reviews": {
            "input": f"{raw_base_path}/olist_order_reviews_dataset.csv",
            "output": f"{bronze_base_path}/reviews",
        },
        "orders": {
            "input": f"{raw_base_path}/olist_orders_dataset.csv",
            "output": f"{bronze_base_path}/orders",
        },
        "products": {
            "input": f"{raw_base_path}/olist_products_dataset.csv",
            "output": f"{bronze_base_path}/products",
        },
        "sellers": {
            "input": f"{raw_base_path}/olist_sellers_dataset.csv",
            "output": f"{bronze_base_path}/sellers",
        },
        "category_translation": {
            "input": f"{raw_base_path}/product_category_name_translation.csv",
            "output": f"{bronze_base_path}/category_translation",
        },
    }

    for table_name, paths in tables.items():
        process_table(
            spark=spark,
            table_name=table_name,
            input_path=paths["input"],
            output_path=paths["output"],
        )

    spark.stop()


if __name__ == "__main__":
    main()