from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    avg,
    col,
    coalesce,
    count,
    current_timestamp,
    datediff,
    desc,
    first,
    lit,
    lower,
    max as spark_max,
    row_number,
    sum as spark_sum,
    to_date,
    trim,
    when,
)


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("Olist Silver Layer Builder")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_bronze_table(spark, table_name):
    return spark.read.parquet(f"data/bronze/{table_name}")


def write_silver_table(df, table_name):
    output_path = f"data/silver/{table_name}"
    df.write.mode("overwrite").parquet(output_path)
    return output_path


def add_silver_metadata(df):
    return (
        df
        .withColumn("_silver_processed_timestamp", current_timestamp())
        .withColumn("_layer", lit("silver"))
    )


def build_silver_customers(customers_df):
    silver_customers = (
        customers_df
        .select(
            "customer_id",
            "customer_unique_id",
            col("customer_zip_code_prefix").alias("customer_zipcode_prefix"),
            lower(trim(col("customer_city"))).alias("customer_city"),
            trim(col("customer_state")).alias("customer_state"),
        )
        .dropDuplicates(["customer_id"])
    )
    return add_silver_metadata(silver_customers)


def build_silver_orders(orders_df):
    silver_orders = (
        orders_df
        .select(
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        )
        .withColumn("order_purchase_date", to_date(col("order_purchase_timestamp")))
        .withColumn("is_delivered", when(col("order_status") == "delivered", lit(1)).otherwise(lit(0)))
        .withColumn("is_approved", when(col("order_approved_at").isNotNull(), lit(1)).otherwise(lit(0)))
        .withColumn("has_carrier_delivery_date", when(col("order_delivered_carrier_date").isNotNull(), lit(1)).otherwise(lit(0)))
        .withColumn("has_customer_delivery_date", when(col("order_delivered_customer_date").isNotNull(), lit(1)).otherwise(lit(0)))
        .withColumn(
            "delivery_delay_days",
            when(
                col("order_delivered_customer_date").isNotNull() & col("order_estimated_delivery_date").isNotNull(),
                datediff(to_date(col("order_delivered_customer_date")), to_date(col("order_estimated_delivery_date")))
            ).otherwise(None)
        )
        .withColumn("is_late_delivery", when(col("delivery_delay_days") > 0, lit(1)).otherwise(lit(0)))
        .withColumn(
            "has_missing_logistics_dates",
            when(
                col("order_approved_at").isNull()
                | col("order_delivered_carrier_date").isNull()
                | col("order_delivered_customer_date").isNull(),
                lit(1)
            ).otherwise(lit(0))
        )
        .dropDuplicates(["order_id"])
    )
    return add_silver_metadata(silver_orders)


def build_silver_order_items(order_items_df):
    silver_order_items = (
        order_items_df
        .select(
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "shipping_limit_date",
            "price",
            "freight_value",
        )
        .withColumn("item_total_value", col("price") + col("freight_value"))
        .withColumn("has_negative_price", when(col("price") < 0, lit(1)).otherwise(lit(0)))
        .withColumn("has_negative_freight", when(col("freight_value") < 0, lit(1)).otherwise(lit(0)))
        .dropDuplicates(["order_id", "order_item_id"])
    )
    return add_silver_metadata(silver_order_items)


def build_silver_payments(payments_df):
    payment_type_window = Window.partitionBy("order_id").orderBy(desc("payment_value"), "payment_type")

    payment_type_ranked = (
        payments_df
        .withColumn("payment_type_rank", row_number().over(payment_type_window))
        .filter(col("payment_type_rank") == 1)
        .select("order_id", col("payment_type").alias("main_payment_type"))
    )

    payments_agg = (
        payments_df
        .groupBy("order_id")
        .agg(
            spark_sum("payment_value").alias("total_payment_value"),
            count("payment_sequential").alias("payment_count"),
            spark_max("payment_installments").alias("max_payment_installments"),
        )
        .join(payment_type_ranked, on="order_id", how="left")
        .withColumn("has_multiple_payments", when(col("payment_count") > 1, lit(1)).otherwise(lit(0)))
    )
    return add_silver_metadata(payments_agg)


def build_silver_reviews(reviews_df):
    review_window = Window.partitionBy("order_id").orderBy(desc("review_answer_timestamp"), desc("review_creation_date"))

    silver_reviews = (
        reviews_df
        .withColumn("review_rank", row_number().over(review_window))
        .filter(col("review_rank") == 1)
        .select(
            "review_id",
            "order_id",
            "review_score",
            "review_comment_title",
            "review_comment_message",
            "review_creation_date",
            "review_answer_timestamp",
        )
        .withColumn(
            "has_review_comment",
            when(
                col("review_comment_title").isNotNull() | col("review_comment_message").isNotNull(),
                lit(1)
            ).otherwise(lit(0))
        )
        .dropDuplicates(["order_id"])
    )
    return add_silver_metadata(silver_reviews)


def build_silver_products(products_df, category_translation_df):
    products_clean = (
        products_df
        .select(
            "product_id",
            "product_category_name",
            col("product_name_lenght").alias("product_name_length"),
            col("product_description_lenght").alias("product_description_length"),
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        )
        .withColumn("has_missing_category", when(col("product_category_name").isNull(), lit(1)).otherwise(lit(0)))
        .withColumn("product_category_name", coalesce(col("product_category_name"), lit("unknown")))
        .withColumn(
            "has_missing_dimensions",
            when(
                col("product_weight_g").isNull()
                | col("product_length_cm").isNull()
                | col("product_height_cm").isNull()
                | col("product_width_cm").isNull(),
                lit(1)
            ).otherwise(lit(0))
        )
        .withColumn(
            "has_missing_product_metadata",
            when(
                col("product_name_length").isNull()
                | col("product_description_length").isNull()
                | col("product_photos_qty").isNull(),
                lit(1)
            ).otherwise(lit(0))
        )
        .dropDuplicates(["product_id"])
    )

    category_clean = category_translation_df.select(
        "product_category_name",
        "product_category_name_english"
    ).dropDuplicates(["product_category_name"])

    silver_products = (
        products_clean
        .join(category_clean, on="product_category_name", how="left")
        .withColumn("product_category_name_english", coalesce(col("product_category_name_english"), lit("unknown")))
    )
    return add_silver_metadata(silver_products)


def build_silver_sellers(sellers_df):
    silver_sellers = (
        sellers_df
        .select(
            "seller_id",
            col("seller_zip_code_prefix").alias("seller_zipcode_prefix"),
            lower(trim(col("seller_city"))).alias("seller_city"),
            trim(col("seller_state")).alias("seller_state"),
        )
        .dropDuplicates(["seller_id"])
    )
    return add_silver_metadata(silver_sellers)


def build_silver_geolocation(geolocation_df):
    silver_geolocation = (
        geolocation_df
        .groupBy("geolocation_zip_code_prefix")
        .agg(
            avg("geolocation_lat").alias("avg_lat"),
            avg("geolocation_lng").alias("avg_lng"),
            first(lower(trim(col("geolocation_city"))), ignorenulls=True).alias("city"),
            first(trim(col("geolocation_state")), ignorenulls=True).alias("state"),
            count(lit(1)).alias("geolocation_record_count"),
        )
        .withColumnRenamed("geolocation_zip_code_prefix", "zipcode_prefix")
    )
    return add_silver_metadata(silver_geolocation)


def process_and_write(table_name, df):
    output_path = write_silver_table(df, table_name)
    written_count = df.count()
    print(f"Silver table written: {table_name}")
    print(f"Output path: {output_path}")
    print(f"Rows: {written_count}")
    print("-" * 80)


def main():
    spark = create_spark_session()

    customers = read_bronze_table(spark, "customers")
    geolocation = read_bronze_table(spark, "geolocation")
    order_items = read_bronze_table(spark, "order_items")
    payments = read_bronze_table(spark, "payments")
    reviews = read_bronze_table(spark, "reviews")
    orders = read_bronze_table(spark, "orders")
    products = read_bronze_table(spark, "products")
    sellers = read_bronze_table(spark, "sellers")
    category_translation = read_bronze_table(spark, "category_translation")

    process_and_write("customers", build_silver_customers(customers))
    process_and_write("orders", build_silver_orders(orders))
    process_and_write("order_items", build_silver_order_items(order_items))
    process_and_write("payments", build_silver_payments(payments))
    process_and_write("reviews", build_silver_reviews(reviews))
    process_and_write("products", build_silver_products(products, category_translation))
    process_and_write("sellers", build_silver_sellers(sellers))
    process_and_write("geolocation_zipcode", build_silver_geolocation(geolocation))

    spark.stop()


if __name__ == "__main__":
    main()
