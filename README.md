# Snowpark Python Workshop (30 Minutes)

## Story: Building a Data Pipeline for ShopStream Retail

ShopStream is a growing e-commerce company. They need to:
1. Analyze customer purchasing behavior
2. Calculate discount tiers based on lifetime spend
3. Generate monthly revenue summaries by region

You'll build this end-to-end using **Snowpark Python** - from raw data exploration to a production-ready stored procedure.

---

## Prerequisites

- Python 3.8+ installed
- `snowflake-snowpark-python` package (`pip install snowflake-snowpark-python`)
- A Snowflake account with SYSADMIN access (or equivalent)
- Basic SQL and Python knowledge

---

## Agenda (30 Minutes)

| Time | Topic | File |
|------|-------|------|
| 0-5 min | Setup: Create database, tables, load sample data | `01_setup.sql` |
| 5-15 min | DataFrame API: Load, filter, join, aggregate | `02_dataframes.py` |
| 15-20 min | User-Defined Functions (UDFs) | `03_udf_example.py` |
| 20-28 min | Stored Procedures: Full ETL pipeline | `04_stored_procedure.py` |
| 28-30 min | Quiz | `quiz.md` |

The notebook `05_workshop_notebook.ipynb` combines all steps into a single guided flow.

---

## Quick Start

```bash
# 1. Run the SQL setup in Snowsight or SnowSQL
#    Open 01_setup.sql and execute all statements

# 2. Install dependencies
pip install snowflake-snowpark-python

# 3. Run scripts in order
python 02_dataframes.py
python 03_udf_example.py
python 04_stored_procedure.py
```

---

## Connection Setup

All Python scripts use the connection name pattern. Create a `~/.snowflake/connections.toml` file:

```toml
[default]
account = "your_account"
user = "your_user"
password = "your_password"
role = "SYSADMIN"
warehouse = "SHOPSTREAM_WH"
database = "SHOPSTREAM"
schema = "RAW"
```

Or set environment variables: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`.

---

## Key Concepts Covered

1. **Session & Connection** - How to connect to Snowflake from Python
2. **DataFrames** - Lazy evaluation, column expressions, transformations
3. **UDFs** - Extending SQL with Python logic (scalar & vectorized)
4. **Stored Procedures** - Deploying full pipelines as callable procedures
5. **Best Practices** - Performance, testing, deployment patterns
