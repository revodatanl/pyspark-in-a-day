# Databricks notebook source
# MAGIC %md
# MAGIC # Actions vs. Transformation
# MAGIC - **Transformations are lazy**, meaning Spark only remembers the operations but does not execute them until needed.
# MAGIC - **Actions are eager**, meaning they trigger executing, causing Spark to apply all queued transformations in an optimized way.
# MAGIC
# MAGIC # **Examples of Transformations vs. Actions in PySpark**
# MAGIC
# MAGIC
# MAGIC | **Transformation** | **Description** |
# MAGIC |--------------------|----------------|
# MAGIC | `filter()` | Filters rows based on a condition. |
# MAGIC | `select()` | Selects specific columns from a DataFrame. |
# MAGIC | `withColumn()` | Adds or modifies a column in a DataFrame. |
# MAGIC | `drop()` | Removes columns from a DataFrame. |
# MAGIC | `groupBy()` | Groups rows based on a column. |
# MAGIC | `orderBy()` | Sorts the DataFrame based on columns. |
# MAGIC | `distinct()` | Returns distinct rows from a DataFrame. |
# MAGIC | `join()` | Joins two DataFrames on a column. |
# MAGIC | `limit()` | Returns only a specified number of rows (but doesn't trigger execution). |
# MAGIC | `repartition()` | Changes the number of partitions in a DataFrame (causes a shuffle). |
# MAGIC | `coalesce()` | Reduces the number of partitions without causing a shuffle. |
# MAGIC | `union()` | Combines two DataFrames with the same schema. |
# MAGIC | `explode()` | Converts an array column into multiple rows. |
# MAGIC | `map()` | Applies a function to each row (RDD-level transformation). |
# MAGIC | `flatMap()` | Similar to `map()`, but flattens nested results. |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## **Actions (Trigger Execution)**
# MAGIC | **Action** | **Description** |
# MAGIC |-----------|----------------|
# MAGIC | `show()` | Displays the first few rows of the DataFrame. |
# MAGIC | `count()` | Returns the number of rows in the DataFrame. |
# MAGIC | `collect()` | Returns all rows as a list (caution: large datasets!). |
# MAGIC | `take(n)` | Returns the first `n` rows as a list. |
# MAGIC | `first()` | Returns the first row of the DataFrame. |
# MAGIC | `head(n)` | Similar to `take(n)`, returns the first `n` rows. |
# MAGIC | `foreach()` | Applies a function to each row (without returning anything). |
# MAGIC | `foreachPartition()` | Similar to `foreach()`, but operates on partitions. |
# MAGIC | `reduce()` | Aggregates elements using a function (RDD-level action). |
# MAGIC | `saveAsTextFile()` | Saves an RDD as a text file. |
# MAGIC | `saveAsTable()` | Saves a DataFrame as a table in Hive or Databricks. |
# MAGIC | `write.format().save()` | Writes a DataFrame to storage (e.g., Parquet, JSON). |
# MAGIC | `describe()` | Computes summary statistics for numerical columns. |
# MAGIC | `summary()` | Provides extended statistics (mean, stddev, etc.). |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Understanding transformations vs. actions** is crucial for **optimizing Spark jobs** and **reducing unnecessary computation**.
# MAGIC

# COMMAND ----------

df_sales_customers = spark.read.table("samples.bakehouse.sales_customers")
df_sales_suppliers = spark.read.table("samples.bakehouse.sales_suppliers")
df_sales_franchises = spark.read.table("samples.bakehouse.sales_franchises")
df_sales_transactions = spark.read.table("samples.bakehouse.sales_transactions")

# COMMAND ----------

# CTRL ENTER = executing
df_sales_customers_2 = df_sales_customers.filter("gender = 'female'")

df_sales_customers_2.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC # Execution plan example -> Filtering

# COMMAND ----------

from pyspark.sql.functions import col

# Simple transformation: Select specific columns and filter rows
df_sales_transactions_filtered = df_sales_transactions.select("transactionID", "totalPrice").filter(col("totalPrice") > 50)

# Execute an explain on the transformed DataFrame
df_sales_transactions_filtered.explain(True)

# COMMAND ----------

from pyspark.sql.functions import col

# Simple transformation: Select specific columns and filter rows
df_sales_transactions_filtered = df_sales_transactions.select("transactionID", "totalPrice").filter(col("totalPrice") > 50).filter(col("totalPrice") > 25)

# Execute an explain on the transformed DataFrame
df_sales_transactions_filtered.explain(True)

# COMMAND ----------

display(df_sales_transactions_filtered)

# COMMAND ----------

df_sales_transactions_filtered.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC # Shuffle operations

# COMMAND ----------

df_joined = df_sales_transactions.join(df_sales_customers, how="left", on="customerID") \
    .join(df_sales_franchises, how="left",on="franchiseID") \
    .select("product") \
    .distinct()

# COMMAND ----------

df_joined.explain(True)

# COMMAND ----------

df_joined.show()

# COMMAND ----------

df_joined.explain(True)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, max, min, date_format

# 1. Group by franchiseID and product to get total quantity and revenue per product per franchise
df_grouped = df_sales_transactions.groupBy("franchiseID", "product").agg(
    sum("quantity").alias("total_quantity"),
    sum("totalPrice").alias("total_revenue"),
    avg("unitPrice").alias("avg_unit_price")
)

# 2. Pivot the product column to see revenue per franchise per product
df_pivoted = df_sales_transactions.groupBy("franchiseID").pivot("product").sum("totalPrice")

# 3. Aggregate sales by month and payment method
df_monthly_sales = df_sales_transactions.withColumn("month", date_format(col("dateTime"), "yyyy-MM")).groupBy("month", "paymentMethod").agg(
    sum("totalPrice").alias("total_revenue"),
    count("transactionID").alias("total_transactions")
)

# 4. Find the top-selling product per franchise (using window functions)
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("franchiseID").orderBy(col("total_quantity").desc())

df_top_products = df_grouped.withColumn("rank", row_number().over(window_spec)).filter(col("rank") == 1).drop("rank")

# 5. Join with franchise data (assuming df_franchises contains franchise info)

df_sales_with_franchise = df_grouped.join(df_sales_franchises, "franchiseID", "left")



# COMMAND ----------

df_sales_with_franchise.explain(True)

# COMMAND ----------

display(df_sales_with_franchise)

# COMMAND ----------

