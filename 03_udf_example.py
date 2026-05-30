"""
Snowpark Workshop - Part 3: User-Defined Functions (UDFs)
==========================================================
Learn how to extend Snowflake's SQL engine with custom Python logic
using scalar UDFs and vectorized (Pandas) UDFs.

Key Concepts:
- Scalar UDF: processes one row at a time
- Vectorized UDF: processes batches using Pandas (much faster for large data)
- Registration: permanent vs temporary UDFs
- Calling UDFs in DataFrame operations
"""

from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, sum, udf, pandas_udf
from snowflake.snowpark.types import (
    StringType, FloatType, IntegerType,
    PandasSeriesType
)


def main(session: Session) -> None:
    """Demonstrates UDF creation and usage."""

    print("=" * 60)
    print("PART 3: User-Defined Functions (UDFs)")
    print("=" * 60)

    # ─────────────────────────────────────────────────────────
    # 1. SCALAR UDF - Discount Tier Calculator
    # ─────────────────────────────────────────────────────────
    print("\n--- 1. Scalar UDF: Discount Tier ---")
    print("Business Rule: Assign discount tiers based on total spend")
    print("  - Platinum: spend >= $500  → 15% discount")
    print("  - Gold:     spend >= $200  → 10% discount")
    print("  - Silver:   spend >= $100  → 5% discount")
    print("  - Bronze:   spend < $100   → 0% discount")

    # Define the UDF using the @udf decorator
    @udf(
        name="discount_tier",
        is_permanent=False,  # Temporary: only lives for this session
        replace=True,
        input_types=[FloatType()],
        return_type=StringType()
    )
    def discount_tier(total_spend: float) -> str:
        if total_spend >= 500:
            return "Platinum"
        elif total_spend >= 200:
            return "Gold"
        elif total_spend >= 100:
            return "Silver"
        else:
            return "Bronze"

    # Calculate total spend per customer
    orders = session.table("SHOPSTREAM.RAW.ORDERS")
    products = session.table("SHOPSTREAM.RAW.PRODUCTS")
    customers = session.table("SHOPSTREAM.RAW.CUSTOMERS")

    customer_spend = (
        orders
        .join(products, orders["PRODUCT_ID"] == products["PRODUCT_ID"])
        .group_by(orders["CUSTOMER_ID"])
        .agg(sum(col("QUANTITY") * col("UNIT_PRICE")).alias("TOTAL_SPEND"))
    )

    # Apply the UDF to assign discount tiers
    customer_tiers = (
        customer_spend
        .join(customers, customer_spend["CUSTOMER_ID"] == customers["CUSTOMER_ID"])
        .select(
            customers["FIRST_NAME"],
            customers["LAST_NAME"],
            customers["REGION"],
            customer_spend["TOTAL_SPEND"],
            discount_tier(col("TOTAL_SPEND")).alias("DISCOUNT_TIER")
        )
        .sort(col("TOTAL_SPEND").desc())
    )

    print("\nCustomer discount tiers:")
    customer_tiers.show(15)

    # Summary of tier distribution
    tier_summary = (
        customer_tiers
        .group_by("DISCOUNT_TIER")
        .agg(
            col("DISCOUNT_TIER"),  # just for grouping
            sum(col("TOTAL_SPEND")).alias("TIER_TOTAL_SPEND")
        )
    )

    # Simpler approach for tier counts
    print("\nTier distribution:")
    customer_tiers.group_by("DISCOUNT_TIER").count().show()

    # ─────────────────────────────────────────────────────────
    # 2. SCALAR UDF - Email Domain Extractor
    # ─────────────────────────────────────────────────────────
    print("\n--- 2. Scalar UDF: Email Domain Extractor ---")

    @udf(
        name="extract_domain",
        is_permanent=False,
        replace=True,
        input_types=[StringType()],
        return_type=StringType()
    )
    def extract_domain(email: str) -> str:
        if email and "@" in email:
            return email.split("@")[1]
        return "unknown"

    # Apply to customers
    customer_domains = customers.select(
        col("FIRST_NAME"),
        col("EMAIL"),
        extract_domain(col("EMAIL")).alias("DOMAIN")
    )
    print("\nCustomer email domains:")
    customer_domains.show(10)

    # ─────────────────────────────────────────────────────────
    # 3. VECTORIZED UDF (Pandas UDF) - Discount Amount Calculator
    # ─────────────────────────────────────────────────────────
    print("\n--- 3. Vectorized UDF: Discount Calculator ---")
    print("Vectorized UDFs process batches with Pandas → much faster!")

    @pandas_udf(
        name="calculate_discount_amount",
        is_permanent=False,
        replace=True,
        input_types=[PandasSeriesType(FloatType()), PandasSeriesType(StringType())],
        return_type=PandasSeriesType(FloatType())
    )
    def calculate_discount_amount(spend, tier):
        """Calculate actual discount dollar amount based on tier."""
        import pandas as pd

        discount_rates = {
            "Platinum": 0.15,
            "Gold": 0.10,
            "Silver": 0.05,
            "Bronze": 0.00
        }
        rates = tier.map(discount_rates).fillna(0.0)
        return spend * rates

    # Apply vectorized UDF
    customer_discounts = customer_tiers.with_column(
        "DISCOUNT_AMOUNT",
        calculate_discount_amount(col("TOTAL_SPEND"), col("DISCOUNT_TIER"))
    ).select(
        "FIRST_NAME", "TOTAL_SPEND", "DISCOUNT_TIER", "DISCOUNT_AMOUNT"
    ).sort(col("DISCOUNT_AMOUNT").desc())

    print("\nCustomer discounts (vectorized calculation):")
    customer_discounts.show(15)

    # ─────────────────────────────────────────────────────────
    # 4. CALLING UDFs IN SQL
    # ─────────────────────────────────────────────────────────
    print("\n--- 4. Calling UDFs via SQL ---")
    print("Registered UDFs can also be called from SQL queries:")

    sql_result = session.sql("""
        SELECT
            'Alice' AS name,
            discount_tier(350.0) AS tier_for_350,
            discount_tier(150.0) AS tier_for_150,
            discount_tier(50.0) AS tier_for_50
    """)
    sql_result.show()

    print("\n" + "=" * 60)
    print("KEY TAKEAWAYS:")
    print("  - Scalar UDFs: simple, one-row-at-a-time processing")
    print("  - Vectorized UDFs: use Pandas for batch processing (faster)")
    print("  - is_permanent=False → lives only for the session")
    print("  - is_permanent=True → persisted in Snowflake for reuse")
    print("  - UDFs run ON Snowflake, not on your local machine")
    print("=" * 60)


# ─────────────────────────────────────────────────────────────
# Local execution entry point
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    with Session.builder.config("connection_name", "default").getOrCreate() as session:
        main(session)
