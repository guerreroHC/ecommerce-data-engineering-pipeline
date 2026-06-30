import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "olist_warehouse")
POSTGRES_USER = os.getenv("POSTGRES_USER", "olist_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "olist_password")
POSTGRES_SCHEMA = os.getenv("POSTGRES_SCHEMA", "gold")

GOLD_BASE_PATH = Path("data/gold")

TABLES_TO_LOAD = {
    "dim_customers": GOLD_BASE_PATH / "dim_customers",
    "dim_products": GOLD_BASE_PATH / "dim_products",
    "dim_sellers": GOLD_BASE_PATH / "dim_sellers",
    "dim_dates": GOLD_BASE_PATH / "dim_dates",
    "fact_orders": GOLD_BASE_PATH / "fact_orders",
    "fact_order_items": GOLD_BASE_PATH / "fact_order_items",
    "fact_payments": GOLD_BASE_PATH / "fact_payments",
    "fact_reviews": GOLD_BASE_PATH / "fact_reviews",
    "mart_sales_by_month": GOLD_BASE_PATH / "mart_sales_by_month",
    "customer_abt": GOLD_BASE_PATH / "customer_abt",
}


def get_engine():
    connection_url = (
        f"postgresql+pg8000://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
        f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )
    return create_engine(connection_url)


def ensure_schema(engine):
    with engine.begin() as connection:
        connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {POSTGRES_SCHEMA}"))


def load_parquet_to_postgres(engine, table_name, dataset_path):
    if not dataset_path.exists():
        raise FileNotFoundError(f"Dataset path does not exist: {dataset_path}")

    print("=" * 80)
    print(f"Loading table: {POSTGRES_SCHEMA}.{table_name}")
    print(f"Source path: {dataset_path}")

    df = pd.read_parquet(dataset_path)

    print(f"Rows to load: {len(df)}")
    print(f"Columns: {list(df.columns)}")

    max_parameters = 60000
    column_count = len(df.columns)
    safe_chunksize = max(1, min(1000, max_parameters // column_count))

    print(f"Safe chunksize selected: {safe_chunksize}")

    df.to_sql(
        name=table_name,
        con=engine,
        schema=POSTGRES_SCHEMA,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=safe_chunksize,
    )

    print(f"Loaded successfully: {POSTGRES_SCHEMA}.{table_name}")


def main():
    engine = get_engine()
    ensure_schema(engine)

    for table_name, dataset_path in TABLES_TO_LOAD.items():
        load_parquet_to_postgres(engine, table_name, dataset_path)

    print("\n" + "=" * 80)
    print("PostgreSQL warehouse load completed successfully.")
    print("=" * 80)


if __name__ == "__main__":
    main()
