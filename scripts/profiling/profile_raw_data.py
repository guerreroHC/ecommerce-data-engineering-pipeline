from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, min, max


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("Olist Raw Data Profiling")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_csv(spark, path):
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("multiLine", True)
        .option("escape", '"')
        .csv(path)
    )


def basic_profile(df, table_name):
    print("\n" + "=" * 80)
    print(f"TABLE: {table_name}")
    print("=" * 80)

    row_count = df.count()
    column_count = len(df.columns)

    print(f"Rows: {row_count}")
    print(f"Columns: {column_count}")

    print("\nSchema:")
    df.printSchema()

    print("\nSample:")
    df.show(5, truncate=False)

    print("\nNull count by column:")
    null_exprs = [
        count(col(c)).alias(c)
        for c in df.columns
    ]

    non_null_counts = df.select(null_exprs).collect()[0].asDict()

    for column_name in df.columns:
        non_null = non_null_counts[column_name]
        nulls = row_count - non_null
        null_percentage = (nulls / row_count) * 100 if row_count > 0 else 0

        print(f"- {column_name}: {nulls} nulls ({null_percentage:.2f}%)")

    print("\nDistinct count by column:")
    for column_name in df.columns:
        distinct_count = df.select(column_name).distinct().count()
        print(f"- {column_name}: {distinct_count} distinct values")

    print("\nPossible primary keys:")
    for column_name in df.columns:
        distinct_count = df.select(column_name).distinct().count()
        if distinct_count == row_count:
            print(f"- {column_name} appears to be unique")


def temporal_profile(df, table_name, date_columns):
    print("\n" + "-" * 80)
    print(f"TEMPORAL PROFILE: {table_name}")
    print("-" * 80)

    for date_col in date_columns:
        if date_col in df.columns:
            print(f"\nColumn: {date_col}")
            df.select(
                min(col(date_col)).alias("min_date"),
                max(col(date_col)).alias("max_date")
            ).show(truncate=False)


def relationship_checks(tables):
    print("\n" + "=" * 80)
    print("RELATIONSHIP CHECKS")
    print("=" * 80)

    orders = tables["orders"]
    customers = tables["customers"]
    order_items = tables["order_items"]
    payments = tables["payments"]
    reviews = tables["reviews"]
    products = tables["products"]
    sellers = tables["sellers"]

    print("\n1. Orders without customer:")
    orders_without_customer = (
        orders.alias("o")
        .join(customers.alias("c"), col("o.customer_id") == col("c.customer_id"), "left_anti")
        .count()
    )
    print(f"Orders without customer: {orders_without_customer}")

    print("\n2. Customers without orders:")
    customers_without_orders = (
        customers.alias("c")
        .join(orders.alias("o"), col("c.customer_id") == col("o.customer_id"), "left_anti")
        .count()
    )
    print(f"Customers without orders: {customers_without_orders}")

    print("\n3. Orders without items:")
    orders_without_items = (
        orders.alias("o")
        .join(order_items.alias("i"), col("o.order_id") == col("i.order_id"), "left_anti")
        .count()
    )
    print(f"Orders without items: {orders_without_items}")

    print("\n4. Orders without payments:")
    orders_without_payments = (
        orders.alias("o")
        .join(payments.alias("p"), col("o.order_id") == col("p.order_id"), "left_anti")
        .count()
    )
    print(f"Orders without payments: {orders_without_payments}")

    print("\n5. Orders without reviews:")
    orders_without_reviews = (
        orders.alias("o")
        .join(reviews.alias("r"), col("o.order_id") == col("r.order_id"), "left_anti")
        .count()
    )
    print(f"Orders without reviews: {orders_without_reviews}")

    print("\n6. Order items without product:")
    items_without_product = (
        order_items.alias("i")
        .join(products.alias("p"), col("i.product_id") == col("p.product_id"), "left_anti")
        .count()
    )
    print(f"Order items without product: {items_without_product}")

    print("\n7. Order items without seller:")
    items_without_seller = (
        order_items.alias("i")
        .join(sellers.alias("s"), col("i.seller_id") == col("s.seller_id"), "left_anti")
        .count()
    )
    print(f"Order items without seller: {items_without_seller}")

    print("\n8. Number of items per order:")
    (
        order_items
        .groupBy("order_id")
        .count()
        .orderBy(col("count").desc())
        .show(10, truncate=False)
    )

    print("\n9. Number of orders per customer_id:")
    (
        orders
        .groupBy("customer_id")
        .count()
        .orderBy(col("count").desc())
        .show(10, truncate=False)
    )

    print("\n10. Number of orders per customer_unique_id:")
    (
        orders.alias("o")
        .join(customers.alias("c"), col("o.customer_id") == col("c.customer_id"), "inner")
        .groupBy("customer_unique_id")
        .count()
        .orderBy(col("count").desc())
        .show(10, truncate=False)
    )


def main():
    spark = create_spark_session()

    base_path = "data/raw/olist"

    paths = {
        "customers": f"{base_path}/olist_customers_dataset.csv",
        "geolocation": f"{base_path}/olist_geolocation_dataset.csv",
        "order_items": f"{base_path}/olist_order_items_dataset.csv",
        "payments": f"{base_path}/olist_order_payments_dataset.csv",
        "reviews": f"{base_path}/olist_order_reviews_dataset.csv",
        "orders": f"{base_path}/olist_orders_dataset.csv",
        "products": f"{base_path}/olist_products_dataset.csv",
        "sellers": f"{base_path}/olist_sellers_dataset.csv",
        "category_translation": f"{base_path}/product_category_name_translation.csv",
    }

    tables = {}

    for table_name, path in paths.items():
        tables[table_name] = load_csv(spark, path)

    for table_name, df in tables.items():
        basic_profile(df, table_name)

    temporal_profile(
        tables["orders"],
        "orders",
        [
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
    )

    temporal_profile(
        tables["order_items"],
        "order_items",
        [
            "shipping_limit_date",
        ],
    )

    temporal_profile(
        tables["reviews"],
        "reviews",
        [
            "review_creation_date",
            "review_answer_timestamp",
        ],
    )

    relationship_checks(tables)

    spark.stop()


if __name__ == "__main__":
    main()