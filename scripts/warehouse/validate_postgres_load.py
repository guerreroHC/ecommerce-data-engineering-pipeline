import os

import pandas as pd
from sqlalchemy import create_engine, text

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "olist_warehouse")
POSTGRES_USER = os.getenv("POSTGRES_USER", "olist_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "olist_password")
POSTGRES_SCHEMA = os.getenv("POSTGRES_SCHEMA", "gold")

EXPECTED_COUNTS = {
    "dim_customers": 96096,
    "dim_products": 32951,
    "dim_sellers": 3095,
    "dim_dates": 634,
    "fact_orders": 99441,
    "fact_order_items": 112650,
    "fact_payments": 99440,
    "fact_reviews": 98673,
    "mart_sales_by_month": 24,
    "customer_abt": 96096,
}


def get_engine():
    connection_url = (
        f"postgresql+pg8000://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
        f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )
    return create_engine(connection_url)


def table_exists(engine, table_name):
    query = text("""
        SELECT COUNT(*) AS table_count
        FROM information_schema.tables
        WHERE table_schema = :schema_name
          AND table_name = :table_name
    """)
    with engine.begin() as connection:
        result = connection.execute(
            query,
            {"schema_name": POSTGRES_SCHEMA, "table_name": table_name},
        ).scalar()
    return result == 1


def get_row_count(engine, table_name):
    query = text(f'SELECT COUNT(*) FROM {POSTGRES_SCHEMA}."{table_name}"')
    with engine.begin() as connection:
        return connection.execute(query).scalar()


def main():
    engine = get_engine()
    results = []

    for table_name, expected_count in EXPECTED_COUNTS.items():
        exists = table_exists(engine, table_name)

        if not exists:
            results.append({
                "table_name": table_name,
                "expected_count": expected_count,
                "actual_count": None,
                "exists_status": "FAILED",
                "count_status": "FAILED",
            })
            continue

        actual_count = get_row_count(engine, table_name)
        count_status = "OK" if actual_count == expected_count else "FAILED"

        results.append({
            "table_name": table_name,
            "expected_count": expected_count,
            "actual_count": actual_count,
            "exists_status": "OK",
            "count_status": count_status,
        })

    print("=" * 80)
    print("POSTGRES WAREHOUSE VALIDATION SUMMARY")
    print("=" * 80)

    for result in results:
        print(
            f"{result['table_name']}: "
            f"expected={result['expected_count']}, "
            f"actual={result['actual_count']}, "
            f"exists_status={result['exists_status']}, "
            f"count_status={result['count_status']}"
        )

    failed = [
        result for result in results
        if result["exists_status"] != "OK" or result["count_status"] != "OK"
    ]

    print("\n" + "=" * 80)
    print("FINAL STATUS")
    print("=" * 80)

    if not failed:
        print("PostgreSQL warehouse validation completed successfully.")
    else:
        print("PostgreSQL warehouse validation failed.")
        for result in failed:
            print(
                f"- {result['table_name']}: "
                f"exists_status={result['exists_status']}, "
                f"count_status={result['count_status']}"
            )

    print("\nSample query: customer_abt churn distribution")
    query = f"""
        SELECT churn_flag, COUNT(*) AS customers
        FROM {POSTGRES_SCHEMA}.customer_abt
        GROUP BY churn_flag
        ORDER BY churn_flag
    """
    print(pd.read_sql(query, engine))


if __name__ == "__main__":
    main()
