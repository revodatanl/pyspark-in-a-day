# Databricks notebook source
# MAGIC %md
# MAGIC # Lets explore some sample data
# MAGIC - Can you find the samples catalog?

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading a dataframe from a unity catalog delta table:
# MAGIC - spark.read.table()

# COMMAND ----------

# MAGIC %md
# MAGIC # Basic PySpark DataFrame Operations
# MAGIC For more: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html
# MAGIC ## 1. Inspecting Data
# MAGIC - `df.show(n=20, truncate=True)` – Display the first `n` rows of the DataFrame.
# MAGIC - `df.printSchema()` – Print the schema of the DataFrame.
# MAGIC - `df.columns` – Get a list of column names.
# MAGIC - `df.dtypes` – Get a list of column names and their data types.
# MAGIC - `df.describe().show()` – Compute summary statistics for numerical columns.
# MAGIC - `df.summary().show()` – More detailed statistics including percentiles.
# MAGIC - `df.count()` – Count the number of rows in the DataFrame.
# MAGIC
# MAGIC ## 2. Filtering and Selecting Data
# MAGIC - `df.select("col1", "col2").show()` – Select specific columns.
# MAGIC - `df.filter(df["col"] > 10).show()` – Filter rows based on a condition.
# MAGIC - `df.where(df["col"] == "value").show()` – Another way to filter data.
# MAGIC - `df.dropna()` – Remove rows with null values.
# MAGIC - `df.fillna(value, subset=["col1", "col2"])` – Fill missing values.
# MAGIC - `df.replace({"old_value": "new_value"})` – Replace specific values.
# MAGIC
# MAGIC ## 3. Sorting and Distinct Values
# MAGIC - `df.distinct().show()` – Show distinct rows.
# MAGIC - `df.dropDuplicates(["col1", "col2"])` – Drop duplicate rows based on specific columns.
# MAGIC - `df.orderBy("col", ascending=False).show()` – Sort data by a column.
# MAGIC - `df.sort("col").show()` – Another way to sort data.
# MAGIC
# MAGIC ## 4. Aggregations and Grouping
# MAGIC - `df.groupBy("col").count().show()` – Group by a column and count occurrences.
# MAGIC - `df.groupBy("col").agg({"col2": "sum"}).show()` – Aggregate using built-in functions.
# MAGIC - `df.agg({"col": "mean"}).show()` – Compute column-level aggregations.
# MAGIC
# MAGIC ## 5. Adding and Modifying Columns
# MAGIC - `df.withColumn("new_col", df["col"] * 2).show()` – Add a new column.
# MAGIC - `df.withColumnRenamed("old_name", "new_name")` – Rename a column.
# MAGIC - `df.drop("col")` – Drop a column.
# MAGIC
# MAGIC ## 6. Joins and Merging Data
# MAGIC - `df1.join(df2, df1["id"] == df2["id"], "inner")` – Inner join.
# MAGIC - `df1.join(df2, "id", "left")` – Left join.
# MAGIC - `df1.join(df2, "id", "right")` – Right join.
# MAGIC - `df1.join(df2, "id", "outer")` – Full outer join.
# MAGIC
# MAGIC ## 7. Converting Data
# MAGIC - `df.toPandas()` – Convert to a Pandas DataFrame.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Sample dataset: bakehouse

# COMMAND ----------

from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType

df_sales_customers = spark.read.table("samples.bakehouse.sales_customers")
display(df_sales_customers)
df_sales_customers = df_sales_customers.repartition(8)
# Define a Python function
def replace_customer_id(customer_id):
    customer_id -= 1000000
    return customer_id 

# Convert it into a pandas UDF
replace_customer_id_udf = pandas_udf(replace_customer_id, StringType())

# Apply UDF to DataFrame
df_sales_customers = df_sales_customers.withColumn("customer_id_new", replace_customer_id_udf(df_sales_customers["customerID"]))

display(df_sales_customers)

# COMMAND ----------

df_sales_suppliers = spark.read.table("samples.bakehouse.sales_suppliers")
display(df_sales_suppliers)

# COMMAND ----------

df_sales_franchises = spark.read.table("samples.bakehouse.sales_franchises")
display(df_sales_franchises)

# COMMAND ----------

df_sales_transactions = spark.read.table("samples.bakehouse.sales_transactions")
display(df_sales_transactions)

# COMMAND ----------

# MAGIC %md
# MAGIC # Brainstorm: what business questions can we come up with regarding the bakehouse dataset?
# MAGIC
# MAGIC