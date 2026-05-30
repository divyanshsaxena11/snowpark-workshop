# Snowpark Python Workshop - Quiz (10 MCQs)

Test your understanding of the Snowpark Python API concepts covered in this workshop.

---

## Questions

### Q1. How do you create a Snowpark session using a named connection?

A) `Session.connect("default")`  
B) `Session.builder.config("connection_name", "default").getOrCreate()`  
C) `snowpark.create_session("default")`  
D) `Session("default")`  

---

### Q2. What happens when you call `session.table("MY_TABLE")`?

A) It immediately loads all rows into local memory  
B) It creates a lazy DataFrame — no query is executed yet  
C) It creates a copy of the table in a temporary location  
D) It returns a Python dictionary of the table contents  

---

### Q3. Which function is used to reference a column in Snowpark expressions?

A) `column("name")`  
B) `field("name")`  
C) `col("name")`  
D) `ref("name")`  

---

### Q4. How do you filter rows where the "REGION" column equals "North"?

A) `df.filter(REGION == "North")`  
B) `df.where("REGION = 'North'")`  
C) `df.filter(col("REGION") == "North")`  
D) Both B and C are valid  

---

### Q5. What is the correct way to join two DataFrames in Snowpark?

A) `df1.merge(df2, on="customer_id")`  
B) `df1.join(df2, df1["CUSTOMER_ID"] == df2["CUSTOMER_ID"])`  
C) `pd.merge(df1, df2, on="customer_id")`  
D) `session.join(df1, df2, "customer_id")`  

---

### Q6. What is the key difference between a scalar UDF and a vectorized (Pandas) UDF?

A) Scalar UDFs are faster because they avoid Pandas overhead  
B) Vectorized UDFs process data in batches using Pandas Series, making them faster for large datasets  
C) Scalar UDFs can only return strings  
D) Vectorized UDFs run locally on your machine, not on Snowflake  

---

### Q7. When you register a UDF with `is_permanent=False`, what happens?

A) The UDF is stored permanently but marked as draft  
B) The UDF only exists for the current session and is dropped when the session ends  
C) The UDF is saved to a stage but not callable  
D) The UDF cannot be used in SQL queries  

---

### Q8. What is the correct function signature pattern for a Snowpark stored procedure?

A) `def my_proc(df: DataFrame) -> str`  
B) `def my_proc(session: Session) -> str`  
C) `def my_proc(connection: Connection) -> str`  
D) `def my_proc(**kwargs) -> str`  

---

### Q9. How do you save a DataFrame to a Snowflake table, replacing existing data?

A) `df.to_table("MY_TABLE", mode="replace")`  
B) `df.write.mode("overwrite").save_as_table("MY_TABLE")`  
C) `df.save("MY_TABLE", overwrite=True)`  
D) `session.write_table(df, "MY_TABLE")`  

---

### Q10. Which statement about Snowpark execution is TRUE?

A) DataFrame operations run on your local machine and results are uploaded to Snowflake  
B) UDFs always run faster than native SQL functions  
C) All DataFrame transformations are pushed down to Snowflake's compute engine  
D) You must call `.execute()` on every DataFrame to run the query  

---

---

## Answer Key

| # | Answer | Explanation |
|---|--------|-------------|
| 1 | **B** | `Session.builder.config("connection_name", "default").getOrCreate()` is the standard pattern for creating sessions from named connections in `~/.snowflake/connections.toml`. |
| 2 | **B** | `session.table()` returns a lazy DataFrame. No SQL is sent to Snowflake until an action like `.show()`, `.collect()`, or `.count()` is called. |
| 3 | **C** | `col("name")` from `snowflake.snowpark.functions` is used to reference columns in expressions. |
| 4 | **D** | Both `df.where("REGION = 'North'")` (SQL string) and `df.filter(col("REGION") == "North")` (column expression) are valid. `.filter()` and `.where()` are aliases. |
| 5 | **B** | Snowpark uses `df1.join(df2, condition)` syntax with explicit column equality expressions. |
| 6 | **B** | Vectorized UDFs operate on Pandas Series in batches, reducing serialization overhead and leveraging vectorized operations — significantly faster for large datasets. |
| 7 | **B** | Temporary UDFs (`is_permanent=False`) exist only for the session lifetime. They don't require a stage location and are automatically cleaned up. |
| 8 | **B** | Stored procedures must accept `session: Session` as the first parameter. Snowflake injects the session when the SP is called via `CALL`. |
| 9 | **B** | `df.write.mode("overwrite").save_as_table("MY_TABLE")` is the correct API. Modes include "overwrite", "append", "errorifexists", and "ignore". |
| 10 | **C** | Snowpark pushes all DataFrame operations to Snowflake's compute engine. Your local machine only sends the query plan — Snowflake does the heavy lifting. |

---

## Scoring

| Score | Rating |
|-------|--------|
| 9-10 | Excellent - Ready to build production Snowpark pipelines |
| 7-8 | Good - Solid foundation, review UDF/SP sections |
| 5-6 | Fair - Re-read the DataFrame and UDF examples |
| < 5 | Needs review - Go through the notebook step by step again |
