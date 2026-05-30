"""
Snowpark Workshop - Part 2: DataFrame API Basics
=================================================
Learn how to load, explore, filter, join, and aggregate data
using the Snowpark Python DataFrame API.

Key Concepts:
- Session creation
- Loading tables as DataFrames
- Column expressions with col()
- Filtering, selecting, joining
- Grouping and aggregation
- Lazy evaluation (nothing runs until .show() or .collect())
"""

from snowflake.snowpark import Session
from snowflake.snowpark.functions import (
    col, lit, sum, avg, count, count_distinct,
    date_trunc, when, upper
)


def main(session: Session) -> None:
    """Demonstrates core DataFrame operations."""

    print("=" * 60)
    print("PART 2: Snowpark DataFrame API")
    print("=" * 60)

    # ─────────────────────────────────────────────────────────
    # 1. LOADING DATA
    # ─────────────────────────────────────────────────────────
    print("\n--- 1. Loading Tables ---")

    # session.table() returns a lazy DataFrame (no query runs yet)
    customers = session.table("SHOPSTREAM.RAW.CUSTOMERS")
    products = session.table("SHOPSTREAM.RAW.PRODUCTS")
    orders = session.table("SHOPSTREAM.RAW.ORDERS")

    # .show() triggers execution and prints results
    print("\nCustomers (first 5 rows):")
    customers.show(5)

    print(f"\nTotal customers: {customers.count()}")
    print(f"Total products: {products.count()}")
    print(f"Total orders: {orders.count()}")

    # ─────────────────────────────────────────────────────────
    # 2. SELECT & COLUMN EXPRESSIONS
    # ─────────────────────────────────────────────────────────
    print("\n--- 2. Select & Column Expressions ---")

    # Select specific columns
    customer_names = customers.select(
        col("FIRST_NAME"),
        col("LAST_NAME"),
        col("REGION")
    )
    print("\nCustomer names and regions:")
    customer_names.show(5)

    # Create computed columns with with_column
    products_with_tax = products.with_column(
        "PRICE_WITH_TAX",
        col("UNIT_PRICE") * lit(1.08)  # 8% tax
    )
    print("\nProducts with tax:")
    products_with_tax.select("PRODUCT_NAME", "UNIT_PRICE", "PRICE_WITH_TAX").show(5)

    # ─────────────────────────────────────────────────────────
    # 3. FILTERING
    # ─────────────────────────────────────────────────────────
    print("\n--- 3. Filtering ---")

    # Filter with .filter() or .where() (they are identical)
    north_customers = customers.filter(col("REGION") == "North")
    print(f"\nCustomers in North region: {north_customers.count()}")

    # Multiple conditions
    expensive_electronics = products.filter(
        (col("CATEGORY") == "Electronics") & (col("UNIT_PRICE") > 50)
    )
    print("\nExpensive electronics (> $50):")
    expensive_electronics.select("PRODUCT_NAME", "UNIT_PRICE").show()

    # Using WHEN for conditional logic
    products_tiered = products.select(
        col("PRODUCT_NAME"),
        col("UNIT_PRICE"),
        when(col("UNIT_PRICE") > 200, "Premium")
        .when(col("UNIT_PRICE") > 50, "Standard")
        .otherwise("Budget")
        .alias("PRICE_TIER")
    )
    print("\nProducts with price tiers:")
    products_tiered.show(10)

    # ─────────────────────────────────────────────────────────
    # 4. JOINS
    # ─────────────────────────────────────────────────────────
    print("\n--- 4. Joins ---")

    # Join orders with customers and products
    order_details = (
        orders
        .join(customers, orders["CUSTOMER_ID"] == customers["CUSTOMER_ID"])
        .join(products, orders["PRODUCT_ID"] == products["PRODUCT_ID"])
        .select(
            orders["ORDER_ID"],
            customers["FIRST_NAME"],
            customers["REGION"],
            products["PRODUCT_NAME"],
            products["CATEGORY"],
            orders["QUANTITY"],
            (orders["QUANTITY"] * products["UNIT_PRICE"]).alias("ORDER_TOTAL"),
            orders["ORDER_DATE"]
        )
    )

    print("\nOrder details (first 10):")
    order_details.show(10)

    # ─────────────────────────────────────────────────────────
    # 5. AGGREGATIONS
    # ─────────────────────────────────────────────────────────
    print("\n--- 5. Aggregations ---")

    # Revenue by region
    revenue_by_region = (
        order_details
        .group_by("REGION")
        .agg(
            sum("ORDER_TOTAL").alias("TOTAL_REVENUE"),
            count("ORDER_ID").alias("ORDER_COUNT"),
            avg("ORDER_TOTAL").alias("AVG_ORDER_VALUE")
        )
        .sort(col("TOTAL_REVENUE").desc())
    )
    print("\nRevenue by region:")
    revenue_by_region.show()

    # Monthly revenue trend
    monthly_revenue = (
        order_details
        .group_by(date_trunc("MONTH", col("ORDER_DATE")).alias("MONTH"))
        .agg(
            sum("ORDER_TOTAL").alias("REVENUE"),
            count_distinct("FIRST_NAME").alias("UNIQUE_CUSTOMERS")
        )
        .sort("MONTH")
    )
    print("\nMonthly revenue trend:")
    monthly_revenue.show(12)

    # Top 5 products by revenue
    top_products = (
        order_details
        .group_by("PRODUCT_NAME")
        .agg(sum("ORDER_TOTAL").alias("TOTAL_REVENUE"))
        .sort(col("TOTAL_REVENUE").desc())
        .limit(5)
    )
    print("\nTop 5 products by revenue:")
    top_products.show()

    # ─────────────────────────────────────────────────────────
    # 6. CHAINING OPERATIONS (Putting it all together)
    # ─────────────────────────────────────────────────────────
    print("\n--- 6. Chaining Operations ---")

    # Complex query: Electronics revenue by region, only regions > $1000
    electronics_revenue = (
        orders
        .join(customers, orders["CUSTOMER_ID"] == customers["CUSTOMER_ID"])
        .join(products, orders["PRODUCT_ID"] == products["PRODUCT_ID"])
        .filter(col("CATEGORY") == "Electronics")
        .group_by(upper(col("REGION")).alias("REGION"))
        .agg(
            sum(col("QUANTITY") * col("UNIT_PRICE")).alias("ELECTRONICS_REVENUE"),
            count("ORDER_ID").alias("NUM_ORDERS")
        )
        .filter(col("ELECTRONICS_REVENUE") > 1000)
        .sort(col("ELECTRONICS_REVENUE").desc())
    )
    print("\nElectronics revenue by region (>$1000):")
    electronics_revenue.show()

    print("\n" + "=" * 60)
    print("KEY TAKEAWAYS:")
    print("  - DataFrames are LAZY: no query runs until .show()/.collect()")
    print("  - Use col() to reference columns in expressions")
    print("  - Chain operations: .filter().join().group_by().agg().sort()")
    print("  - Everything runs on Snowflake's engine, not locally")
    print("=" * 60)


# ─────────────────────────────────────────────────────────────
# Local execution entry point
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    with Session.builder.config("connection_name", "snowflake-enabled-trial").getOrCreate() as session:
        main(session)
