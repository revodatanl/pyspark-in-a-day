# Databricks notebook source
# MAGIC %md
# MAGIC # Lets explore some sample data
# MAGIC - Can you find the samples catalog?

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading a dataframe from a unity catalog delta table

# COMMAND ----------

df = spark.read.table("samples.bakehouse.sales_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC # Basic PySpark DataFrame Operations
# MAGIC For more: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Inspecting Data
# MAGIC - `df.show(n=20, truncate=True)` – Display the first `n` rows of the DataFrame.
# MAGIC - `df.printSchema()` – Print the schema of the DataFrame.
# MAGIC - `df.columns` – Get a list of column names.
# MAGIC - `df.dtypes` – Get a list of column names and their data types.
# MAGIC - `df.describe().show()` – Compute summary statistics for numerical columns.
# MAGIC - `df.summary().show()` – More detailed statistics including percentiles.
# MAGIC - `df.count()` – Count the number of rows in the DataFrame.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Filtering and Selecting Data
# MAGIC - `df.select("col1", "col2").show()` – Select specific columns.
# MAGIC - `df.filter(df["col"] > 10).show()` – Filter rows based on a condition.
# MAGIC - `df.where(df["col"] == "value").show()` – Another way to filter data.
# MAGIC - `df.dropna()` – Remove rows with null values.
# MAGIC - `df.fillna(value, subset=["col1", "col2"])` – Fill missing values.
# MAGIC - `df.replace({"old_value": "new_value"})` – Replace specific values.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Sorting and Distinct Values
# MAGIC - `df.distinct().show()` – Show distinct rows.
# MAGIC - `df.dropDuplicates(["col1", "col2"])` – Drop duplicate rows based on specific columns.
# MAGIC - `df.orderBy("col", ascending=False).show()` – Sort data by a column.
# MAGIC - `df.sort("col").show()` – Another way to sort data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Aggregations and Grouping
# MAGIC - `df.groupBy("col").count().show()` – Group by a column and count occurrences.
# MAGIC - `df.groupBy("col").agg({"col2": "sum"}).show()` – Aggregate using built-in functions.
# MAGIC - `df.agg({"col": "mean"}).show()` – Compute column-level aggregations.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Adding and Modifying Columns
# MAGIC - `df.withColumn("new_col", df["col"] * 2).show()` – Add a new column.
# MAGIC - `df.withColumnRenamed("old_name", "new_name")` – Rename a column.
# MAGIC - `df.drop("col")` – Drop a column.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Joins and Merging Data
# MAGIC - `df1.join(df2, df1["id"] == df2["id"], "inner")` – Inner join.
# MAGIC - `df1.join(df2, "id", "left")` – Left join.
# MAGIC - `df1.join(df2, "id", "right")` – Right join.
# MAGIC - `df1.join(df2, "id", "outer")` – Full outer join.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Converting Data
# MAGIC - `df.toPandas()` – Convert to a Pandas DataFrame.

# COMMAND ----------

# MAGIC %md
# MAGIC # Sample dataset: bakehouse

# COMMAND ----------

df = spark.read.table("samples.bakehouse.sales_customers")

# COMMAND ----------

display(df)

# COMMAND ----------

# DBTITLE 1,Create and alter data
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Applying transformations
df_altered = (
    df
    .withColumn("full_name", F.concat(F.col("first_name"), F.lit(" "), F.col("last_name")))
    .filter(F.col("state") == "Kentucky")
    .drop("first_name", "last_name")
)
display(df_altered)

# COMMAND ----------

# DBTITLE 1,Group by and aggregations
# Applying other transformations
df_transformed = (
    df
    .groupBy("state")
    .agg(
        F.count("*").alias("number_of_customers")
    )
    .orderBy(F.col("number_of_customers").desc())
)
display(df_transformed)

# COMMAND ----------

# MAGIC %md
# MAGIC # Brainstorm: what business questions can we come up with regarding the bakehouse dataset?
# MAGIC
# MAGIC Available tables:
# MAGIC - `df_sales_customers`
# MAGIC - `df_sales_suppliers`
# MAGIC - `df_sales_franchises`
# MAGIC - `df_sales_transactions`
# MAGIC

# COMMAND ----------

df_sales_customers = spark.table("samples.bakehouse.sales_customers")
df_sales_suppliers = spark.table("samples.bakehouse.sales_suppliers")
df_sales_franchises = spark.table("samples.bakehouse.sales_franchises")
df_sales_transactions = spark.table("samples.bakehouse.sales_transactions")
