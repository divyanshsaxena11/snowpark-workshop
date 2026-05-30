"""
Snowpark Workshop - Part 4: Stored Procedures
===============================================
Learn how to package a full ETL pipeline as a Snowflake Stored Procedure
that can be scheduled, called from SQL, or triggered by tasks.

Key Concepts:
- The main(session) pattern for SP-compatible code
- Reading from raw tables → transforming → writing to analytics tables
- Registering a stored procedure
- Calling it from SQL
- The dual-mode pattern (works locally AND as a deployed SP)
"""

from snowflake.snowpark import Session
from snowflake.snowpark.functions import (
    col, sum, avg, count, count_distinct,
    date_trunc, when, sproc
)
from snowflake.snowpark.types import StringType, FloatType


def build_monthly_revenue_summary(session: Session) -> str:
    """
    ETL Pipeline: Build Monthly Revenue Summary by Region
    
    Steps:
    1. Load raw tables (customers, products, orders)
    2. Join them into a denormalized order_details view
    3. Aggregate into monthly revenue by region
    4. Add discount tier classification
    5. Save to SHOPSTREAM.ANALYTICS.MONTHLY_REVENUE_SUMMARY
    
    Returns:
        Status message with row count
    """

    # ─── STEP 1: LOAD RAW DATA ───────────────────────────────
    customers = session.table("SHOPSTREAM.RAW.CUSTOMERS")
    products = session.table("SHOPSTREAM.RAW.PRODUCTS")
    orders = session.table("SHOPSTREAM.RAW.ORDERS")

    # ─── STEP 2: JOIN INTO DENORMALIZED VIEW ─────────────────
    order_details = (
        orders
        .join(customers, orders["CUSTOMER_ID"] == customers["CUSTOMER_ID"])
        .join(products, orders["PRODUCT_ID"] == products["PRODUCT_ID"])
        .select(
            orders["ORDER_ID"],
            orders["ORDER_DATE"],
            customers["CUSTOMER_ID"].alias("CUST_ID"),
            customers["FIRST_NAME"],
            customers["REGION"],
            products["PRODUCT_NAME"],
            products["CATEGORY"],
            (orders["QUANTITY"] * products["UNIT_PRICE"]).alias("ORDER_TOTAL")
        )
    )

    # ─── STEP 3: AGGREGATE BY REGION AND MONTH ───────────────
    monthly_summary = (
        order_details
        .group_by(
            col("REGION"),
            date_trunc("MONTH", col("ORDER_DATE")).alias("MONTH")
        )
        .agg(
            sum("ORDER_TOTAL").alias("TOTAL_REVENUE"),
            count("ORDER_ID").alias("ORDER_COUNT"),
            avg("ORDER_TOTAL").alias("AVG_ORDER_VALUE"),
            count_distinct("CUST_ID").alias("UNIQUE_CUSTOMERS")
        )
    )

    # ─── STEP 4: ADD REVENUE TIER CLASSIFICATION ─────────────
    final_summary = monthly_summary.with_column(
        "REVENUE_TIER",
        when(col("TOTAL_REVENUE") > 5000, "High")
        .when(col("TOTAL_REVENUE") > 2000, "Medium")
        .otherwise("Low")
    )

    # ─── STEP 5: SAVE TO ANALYTICS SCHEMA ────────────────────
    target_table = "SHOPSTREAM.ANALYTICS.MONTHLY_REVENUE_SUMMARY"
    final_summary.write.mode("overwrite").save_as_table(target_table)

    # Return status
    row_count = session.table(target_table).count()
    return f"SUCCESS: Saved {row_count} rows to {target_table}"


def build_customer_360(session: Session) -> str:
    """
    ETL Pipeline: Build Customer 360 Profile
    
    Aggregates all customer data into a single profile table with:
    - Total spend, order count, avg order value
    - Discount tier
    - First/last order dates
    """

    orders = session.table("SHOPSTREAM.RAW.ORDERS")
    products = session.table("SHOPSTREAM.RAW.PRODUCTS")
    customers = session.table("SHOPSTREAM.RAW.CUSTOMERS")

    # Calculate customer metrics
    customer_metrics = (
        orders
        .join(products, orders["PRODUCT_ID"] == products["PRODUCT_ID"])
        .group_by(orders["CUSTOMER_ID"])
        .agg(
            sum(col("QUANTITY") * col("UNIT_PRICE")).alias("TOTAL_SPEND"),
            count("ORDER_ID").alias("ORDER_COUNT"),
            avg(col("QUANTITY") * col("UNIT_PRICE")).alias("AVG_ORDER_VALUE")
        )
    )

    # Join with customer details and add tier
    customer_360 = (
        customers
        .join(customer_metrics, customers["CUSTOMER_ID"] == customer_metrics["CUSTOMER_ID"])
        .select(
            customers["CUSTOMER_ID"],
            customers["FIRST_NAME"],
            customers["LAST_NAME"],
            customers["EMAIL"],
            customers["REGION"],
            customers["SIGNUP_DATE"],
            customer_metrics["TOTAL_SPEND"],
            customer_metrics["ORDER_COUNT"],
            customer_metrics["AVG_ORDER_VALUE"],
            when(col("TOTAL_SPEND") >= 500, "Platinum")
            .when(col("TOTAL_SPEND") >= 200, "Gold")
            .when(col("TOTAL_SPEND") >= 100, "Silver")
            .otherwise("Bronze")
            .alias("DISCOUNT_TIER")
        )
    )

    # Save
    target_table = "SHOPSTREAM.ANALYTICS.CUSTOMER_360"
    customer_360.write.mode("overwrite").save_as_table(target_table)

    row_count = session.table(target_table).count()
    return f"SUCCESS: Saved {row_count} rows to {target_table}"


def main(session: Session) -> str:
    """
    Main entry point - orchestrates all ETL pipelines.
    This function is what gets called when deployed as a Stored Procedure.
    """
    print("=" * 60)
    print("PART 4: Stored Procedures - Full ETL Pipeline")
    print("=" * 60)

    # Run pipeline 1: Monthly Revenue Summary
    print("\n--- Running: Monthly Revenue Summary ---")
    result1 = build_monthly_revenue_summary(session)
    print(f"  Result: {result1}")

    # Run pipeline 2: Customer 360
    print("\n--- Running: Customer 360 ---")
    result2 = build_customer_360(session)
    print(f"  Result: {result2}")

    # Show results
    print("\n--- Monthly Revenue Summary (sample) ---")
    session.table("SHOPSTREAM.ANALYTICS.MONTHLY_REVENUE_SUMMARY").sort("MONTH").show(10)

    print("\n--- Customer 360 (top spenders) ---")
    (
        session.table("SHOPSTREAM.ANALYTICS.CUSTOMER_360")
        .sort(col("TOTAL_SPEND").desc())
        .show(10)
    )

    # ─────────────────────────────────────────────────────────
    # REGISTERING AS A STORED PROCEDURE
    # ─────────────────────────────────────────────────────────
    print("\n--- Registering as Stored Procedure ---")

    # Register the pipeline as a callable stored procedure
    session.sproc.register(
        func=build_monthly_revenue_summary,
        name="build_monthly_revenue_summary_sp",
        replace=True,
        is_permanent=False
    )

    session.sproc.register(
        func=build_customer_360,
        name="build_customer_360_sp",
        replace=True,
        is_permanent=False
    )

    print("  Registered: build_monthly_revenue_summary_sp()")
    print("  Registered: build_customer_360_sp()")

    # Call via SQL to demonstrate
    print("\n--- Calling SP via SQL ---")
    call_result = session.sql("CALL build_monthly_revenue_summary_sp()").collect()
    print(f"  SQL CALL result: {call_result[0][0]}")

    print("\n" + "=" * 60)
    print("KEY TAKEAWAYS:")
    print("  - Structure code as main(session) → works as SP AND locally")
    print("  - session.sproc.register() deploys Python as a Stored Procedure")
    print("  - SPs run entirely on Snowflake (no local resources needed)")
    print("  - Use CALL <sp_name>() from SQL to execute")
    print("  - is_permanent=True + stage_location for production deployment")
    print("=" * 60)

    return f"Pipeline complete. {result1} | {result2}"


# ─────────────────────────────────────────────────────────────
# Local execution - this block does NOT run when deployed as SP
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    with Session.builder.config("connection_name", "default").getOrCreate() as session:
        result = main(session)
        print(f"\nFinal result: {result}")
