from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    count,
    countDistinct,
    current_timestamp,
    datediff,
    desc,
    first,
    lit,
    max as spark_max,
    min as spark_min,
    row_number,
    round as spark_round,
    sum as spark_sum,
    when,
)


CHURN_RECENCY_DAYS_THRESHOLD = 180


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("Olist Customer ABT Builder")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_gold_table(spark, table_name):
    return spark.read.parquet(f"data/gold/{table_name}")


def write_abt(df, table_name):
    output_path = f"data/gold/{table_name}"
    df.write.mode("overwrite").parquet(output_path)
    return output_path


def add_abt_metadata(df):
    return (
        df
        .withColumn("_abt_processed_timestamp", current_timestamp())
        .withColumn("_layer", lit("gold"))
    )


def build_customer_order_features(fact_orders):
    max_purchase_date = fact_orders.select(spark_max("order_purchase_date")).collect()[0][0]

    order_features = (
        fact_orders
        .groupBy("customer_unique_id")
        .agg(
            countDistinct("order_id").alias("total_orders"),
            spark_min("order_purchase_date").alias("first_purchase_date"),
            spark_max("order_purchase_date").alias("last_purchase_date"),
            spark_sum(coalesce(col("total_payment_value"), lit(0.0))).alias("total_spent"),
            avg("total_payment_value").alias("avg_order_value"),
            avg("review_score").alias("avg_review_score"),
            spark_sum(coalesce(col("is_late_delivery"), lit(0))).alias("late_deliveries"),
            spark_sum(coalesce(col("is_delivered"), lit(0))).alias("delivered_orders"),
            avg("delivery_delay_days").alias("avg_delivery_delay_days"),
            spark_sum(coalesce(col("has_missing_logistics_dates"), lit(0))).alias("orders_with_missing_logistics_dates"),
        )
        .withColumn("recency_days", datediff(lit(max_purchase_date), col("last_purchase_date")))
        .withColumn("frequency", col("total_orders"))
        .withColumn("monetary_value", col("total_spent"))
        .withColumn(
            "late_delivery_rate",
            when(col("delivered_orders") > 0, col("late_deliveries") / col("delivered_orders")).otherwise(lit(0.0))
        )
        .withColumn(
            "churn_flag",
            when(col("recency_days") > CHURN_RECENCY_DAYS_THRESHOLD, lit(1)).otherwise(lit(0))
        )
    )

    return order_features


def build_customer_item_features(fact_orders, fact_order_items):
    order_customer = fact_orders.select("order_id", "customer_unique_id")

    item_features = (
        fact_order_items
        .join(order_customer, on="order_id", how="left")
        .groupBy("customer_unique_id")
        .agg(
            count("order_item_id").alias("total_items"),
            countDistinct("product_id").alias("distinct_products_purchased"),
            countDistinct("seller_id").alias("distinct_sellers_purchased_from"),
            countDistinct("product_category_name_english").alias("distinct_categories_purchased"),
            spark_sum(coalesce(col("price"), lit(0.0))).alias("total_product_revenue"),
            spark_sum(coalesce(col("freight_value"), lit(0.0))).alias("total_freight_value"),
            spark_sum(coalesce(col("item_total_value"), lit(0.0))).alias("total_item_value"),
        )
    )

    return item_features


def build_most_used_payment_type(fact_orders):
    payment_counts = (
        fact_orders
        .where(col("main_payment_type").isNotNull())
        .groupBy("customer_unique_id", "main_payment_type")
        .agg(count("order_id").alias("payment_type_count"))
    )

    window = Window.partitionBy("customer_unique_id").orderBy(desc("payment_type_count"), "main_payment_type")

    most_used_payment = (
        payment_counts
        .withColumn("payment_rank", row_number().over(window))
        .filter(col("payment_rank") == 1)
        .select(
            "customer_unique_id",
            col("main_payment_type").alias("most_used_payment_type")
        )
    )

    return most_used_payment


def build_favorite_category(fact_orders, fact_order_items):
    order_customer = fact_orders.select("order_id", "customer_unique_id")

    category_counts = (
        fact_order_items
        .join(order_customer, on="order_id", how="left")
        .where(col("product_category_name_english").isNotNull())
        .groupBy("customer_unique_id", "product_category_name_english")
        .agg(count("order_item_id").alias("category_item_count"))
    )

    window = Window.partitionBy("customer_unique_id").orderBy(desc("category_item_count"), "product_category_name_english")

    favorite_category = (
        category_counts
        .withColumn("category_rank", row_number().over(window))
        .filter(col("category_rank") == 1)
        .select(
            "customer_unique_id",
            col("product_category_name_english").alias("favorite_category")
        )
    )

    return favorite_category


def build_customer_abt(dim_customers, fact_orders, fact_order_items):
    order_features = build_customer_order_features(fact_orders)
    item_features = build_customer_item_features(fact_orders, fact_order_items)
    most_used_payment = build_most_used_payment_type(fact_orders)
    favorite_category = build_favorite_category(fact_orders, fact_order_items)

    customer_abt = (
        dim_customers
        .select(
            "customer_unique_id",
            "customer_state",
            "customer_city",
            "customer_zipcode_prefix",
        )
        .join(order_features, on="customer_unique_id", how="left")
        .join(item_features, on="customer_unique_id", how="left")
        .join(most_used_payment, on="customer_unique_id", how="left")
        .join(favorite_category, on="customer_unique_id", how="left")
        .withColumn("total_orders", coalesce(col("total_orders"), lit(0)))
        .withColumn("frequency", coalesce(col("frequency"), lit(0)))
        .withColumn("total_items", coalesce(col("total_items"), lit(0)))
        .withColumn("total_spent", spark_round(coalesce(col("total_spent"), lit(0.0)), 2))
        .withColumn("avg_order_value", spark_round(coalesce(col("avg_order_value"), lit(0.0)), 2))
        .withColumn("avg_review_score", spark_round(col("avg_review_score"), 2))
        .withColumn("monetary_value", spark_round(coalesce(col("monetary_value"), lit(0.0)), 2))
        .withColumn("late_delivery_rate", spark_round(coalesce(col("late_delivery_rate"), lit(0.0)), 4))
        .withColumn("avg_delivery_delay_days", spark_round(col("avg_delivery_delay_days"), 2))
        .withColumn("total_product_revenue", spark_round(coalesce(col("total_product_revenue"), lit(0.0)), 2))
        .withColumn("total_freight_value", spark_round(coalesce(col("total_freight_value"), lit(0.0)), 2))
        .withColumn("total_item_value", spark_round(coalesce(col("total_item_value"), lit(0.0)), 2))
        .withColumn("distinct_products_purchased", coalesce(col("distinct_products_purchased"), lit(0)))
        .withColumn("distinct_sellers_purchased_from", coalesce(col("distinct_sellers_purchased_from"), lit(0)))
        .withColumn("distinct_categories_purchased", coalesce(col("distinct_categories_purchased"), lit(0)))
        .withColumn("orders_with_missing_logistics_dates", coalesce(col("orders_with_missing_logistics_dates"), lit(0)))
        .withColumn("most_used_payment_type", coalesce(col("most_used_payment_type"), lit("unknown")))
        .withColumn("favorite_category", coalesce(col("favorite_category"), lit("unknown")))
    )

    return add_abt_metadata(customer_abt)


def main():
    spark = create_spark_session()

    dim_customers = read_gold_table(spark, "dim_customers")
    fact_orders = read_gold_table(spark, "fact_orders")
    fact_order_items = read_gold_table(spark, "fact_order_items")

    customer_abt = build_customer_abt(dim_customers, fact_orders, fact_order_items)

    output_path = write_abt(customer_abt, "customer_abt")
    row_count = customer_abt.count()

    print("=" * 80)
    print("Customer ABT built successfully")
    print("=" * 80)
    print(f"Output path: {output_path}")
    print(f"Rows: {row_count}")
    print("Columns:")
    print(customer_abt.columns)

    spark.stop()


if __name__ == "__main__":
    main()
