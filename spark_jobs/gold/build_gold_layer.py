from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    current_timestamp,
    dayofmonth,
    dayofweek,
    lit,
    month,
    quarter,
    sum as spark_sum,
    year,
    first,
)


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("Olist Gold Layer Builder")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_silver_table(spark, table_name):
    return spark.read.parquet(f"data/silver/{table_name}")


def write_gold_table(df, table_name):
    output_path = f"data/gold/{table_name}"
    df.write.mode("overwrite").parquet(output_path)
    return output_path


def add_gold_metadata(df):
    return (
        df
        .withColumn("_gold_processed_timestamp", current_timestamp())
        .withColumn("_layer", lit("gold"))
    )


def build_dim_customers(customers_df, orders_df):
    customer_orders = (
        customers_df
        .join(orders_df.select("order_id", "customer_id", "order_purchase_date"), on="customer_id", how="left")
    )

    dim_customers = (
        customer_orders
        .groupBy("customer_unique_id")
        .agg(
            first("customer_id", ignorenulls=True).alias("sample_customer_id"),
            first("customer_zipcode_prefix", ignorenulls=True).alias("customer_zipcode_prefix"),
            first("customer_city", ignorenulls=True).alias("customer_city"),
            first("customer_state", ignorenulls=True).alias("customer_state"),
            count("order_id").alias("total_orders_registered"),
        )
    )
    return add_gold_metadata(dim_customers)


def build_dim_products(products_df):
    dim_products = products_df.select(
        "product_id",
        "product_category_name",
        "product_category_name_english",
        "product_name_length",
        "product_description_length",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
        "has_missing_category",
        "has_missing_product_metadata",
        "has_missing_dimensions",
    ).dropDuplicates(["product_id"])

    return add_gold_metadata(dim_products)


def build_dim_sellers(sellers_df):
    dim_sellers = sellers_df.select(
        "seller_id",
        "seller_zipcode_prefix",
        "seller_city",
        "seller_state",
    ).dropDuplicates(["seller_id"])

    return add_gold_metadata(dim_sellers)


def build_dim_dates(orders_df):
    dim_dates = (
        orders_df
        .select(col("order_purchase_date").alias("date"))
        .where(col("date").isNotNull())
        .dropDuplicates(["date"])
        .withColumn("year", year(col("date")))
        .withColumn("quarter", quarter(col("date")))
        .withColumn("month", month(col("date")))
        .withColumn("day", dayofmonth(col("date")))
        .withColumn("day_of_week", dayofweek(col("date")))
    )

    return add_gold_metadata(dim_dates)


def build_fact_orders(orders_df, customers_df, payments_df, reviews_df):
    fact_orders = (
        orders_df.alias("o")
        .join(customers_df.select("customer_id", "customer_unique_id"), on="customer_id", how="left")
        .join(payments_df.select(
            "order_id",
            "total_payment_value",
            "payment_count",
            "max_payment_installments",
            "main_payment_type",
            "has_multiple_payments",
        ), on="order_id", how="left")
        .join(reviews_df.select(
            "order_id",
            "review_score",
            "has_review_comment",
        ), on="order_id", how="left")
        .select(
            "order_id",
            "customer_id",
            "customer_unique_id",
            "order_status",
            "order_purchase_timestamp",
            "order_purchase_date",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
            "is_delivered",
            "is_approved",
            "has_carrier_delivery_date",
            "has_customer_delivery_date",
            "delivery_delay_days",
            "is_late_delivery",
            "has_missing_logistics_dates",
            "total_payment_value",
            "payment_count",
            "max_payment_installments",
            "main_payment_type",
            "has_multiple_payments",
            "review_score",
            "has_review_comment",
        )
        .dropDuplicates(["order_id"])
    )

    return add_gold_metadata(fact_orders)


def build_fact_order_items(order_items_df, orders_df, products_df, sellers_df):
    fact_order_items = (
        order_items_df.alias("oi")
        .join(orders_df.select("order_id", "customer_id", "order_purchase_date", "order_status"), on="order_id", how="left")
        .join(products_df.select("product_id", "product_category_name_english"), on="product_id", how="left")
        .join(sellers_df.select("seller_id", "seller_state"), on="seller_id", how="left")
        .select(
            "order_id",
            "order_item_id",
            "customer_id",
            "product_id",
            "seller_id",
            "order_purchase_date",
            "order_status",
            "product_category_name_english",
            "seller_state",
            "shipping_limit_date",
            "price",
            "freight_value",
            "item_total_value",
            "has_negative_price",
            "has_negative_freight",
        )
    )

    return add_gold_metadata(fact_order_items)


def build_fact_payments(payments_df):
    fact_payments = payments_df.select(
        "order_id",
        "total_payment_value",
        "payment_count",
        "max_payment_installments",
        "main_payment_type",
        "has_multiple_payments",
    ).dropDuplicates(["order_id"])

    return add_gold_metadata(fact_payments)


def build_fact_reviews(reviews_df):
    fact_reviews = reviews_df.select(
        "review_id",
        "order_id",
        "review_score",
        "has_review_comment",
        "review_creation_date",
        "review_answer_timestamp",
    ).dropDuplicates(["order_id"])

    return add_gold_metadata(fact_reviews)


def build_mart_sales_by_month(fact_order_items_df):
    mart_sales_by_month = (
        fact_order_items_df
        .where(col("order_purchase_date").isNotNull())
        .withColumn("year", year(col("order_purchase_date")))
        .withColumn("month", month(col("order_purchase_date")))
        .groupBy("year", "month")
        .agg(
            count("order_id").alias("total_order_items"),
            spark_sum("price").alias("total_product_revenue"),
            spark_sum("freight_value").alias("total_freight_value"),
            spark_sum("item_total_value").alias("total_item_value"),
        )
    )

    return add_gold_metadata(mart_sales_by_month)


def process_and_write(table_name, df):
    output_path = write_gold_table(df, table_name)
    row_count = df.count()
    print(f"Gold table written: {table_name}")
    print(f"Output path: {output_path}")
    print(f"Rows: {row_count}")
    print("-" * 80)


def main():
    spark = create_spark_session()

    customers = read_silver_table(spark, "customers")
    orders = read_silver_table(spark, "orders")
    order_items = read_silver_table(spark, "order_items")
    payments = read_silver_table(spark, "payments")
    reviews = read_silver_table(spark, "reviews")
    products = read_silver_table(spark, "products")
    sellers = read_silver_table(spark, "sellers")

    dim_customers = build_dim_customers(customers, orders)
    dim_products = build_dim_products(products)
    dim_sellers = build_dim_sellers(sellers)
    dim_dates = build_dim_dates(orders)
    fact_orders = build_fact_orders(orders, customers, payments, reviews)
    fact_order_items = build_fact_order_items(order_items, orders, products, sellers)
    fact_payments = build_fact_payments(payments)
    fact_reviews = build_fact_reviews(reviews)
    mart_sales_by_month = build_mart_sales_by_month(fact_order_items)

    process_and_write("dim_customers", dim_customers)
    process_and_write("dim_products", dim_products)
    process_and_write("dim_sellers", dim_sellers)
    process_and_write("dim_dates", dim_dates)
    process_and_write("fact_orders", fact_orders)
    process_and_write("fact_order_items", fact_order_items)
    process_and_write("fact_payments", fact_payments)
    process_and_write("fact_reviews", fact_reviews)
    process_and_write("mart_sales_by_month", mart_sales_by_month)

    spark.stop()


if __name__ == "__main__":
    main()
