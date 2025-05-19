# Databricks notebook source
# MAGIC %md
# MAGIC # User-Defined functions (UDFs)
# MAGIC User-Defined Functions (UDFs) in PySpark allow you to define custom functions and apply them to DataFrame columns. They are useful when built-in Spark functions do not provide the needed functionality.

# COMMAND ----------

# MAGIC %md
# MAGIC - **PySpark Built-in Methods**:
# MAGIC   - Highly optimized for distributed computing
# MAGIC   - Fast execution and scalability
# MAGIC   - Efficient resource utilization
# MAGIC   - Recommended for best performance
# MAGIC
# MAGIC - **Pandas-on-Spark**:
# MAGIC   - User-friendly, familiar Pandas-like syntax
# MAGIC   - Simplifies transition from Pandas workflows
# MAGIC   - Slightly less performant due to abstraction overhead
# MAGIC   - Convenient for data scientists familiar with pandas
# MAGIC
# MAGIC - **Python UDFs**:
# MAGIC   - High flexibility for custom computations
# MAGIC   - Easier to implement complex logic
# MAGIC   - Typically slower due to serialization overhead
# MAGIC   - Recommended when built-in methods or APIs do not suffice

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explore a simple UDF
# MAGIC
# MAGIC Calculate the square of a number `x^2`

# COMMAND ----------

# DBTITLE 1,Generate data
import pyspark.sql.functions as F
import pyspark.sql.types as T

data = [(1,), (2,), (3,), (4,)]
df = spark.createDataFrame(data, ["number"])

# COMMAND ----------

# DBTITLE 1,Display generated data
display(df)

# COMMAND ----------

# DBTITLE 1,Standard SQL / PySpark
display(
    df
    .withColumn("squared", F.pow(F.col("number"), 2))
)

# COMMAND ----------

# DBTITLE 1,Creating a Python UDF
import pyspark.sql.functions as F
import pyspark.sql.types as T

# Define a Python function
def square(x):
    return x * x
    
# Convert it into a UDF
square_udf = F.udf(square, T.IntegerType())

# Alternative way to define UDF using decorators
F.udf(T.IntegerType())
def square_udf(x):
    return x * x

# COMMAND ----------

display(
    df
    .withColumn("squared", square_udf(F.col("number")))
)

# COMMAND ----------

# DBTITLE 1,Creating a Pandas UDF
@F.pandas_udf(T.IntegerType())
def square_pandas_udf(x):
    return x * x

display(
    df.withColumn("squared", square_pandas_udf(F.col("number")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Lets try it out on the sample data

# COMMAND ----------

# Read from a catalog
df_sales_transactions = spark.read.table("samples.bakehouse.sales_transactions")

# COMMAND ----------

# Define a Python function
F.udf(T.StringType())
def concat_udf(col1, col2):
    return col1 + col2

# Apply UDF to DataFrame
df_sales_transactions = (
    df_sales_transactions
    .withColumn("concatenated", concat_udf(F.col("product"), F.col("paymentMethod")))
)

# COMMAND ----------

display(df_sales_transactions.select("product", "paymentMethod", "concatenated"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Performance differences for SQL vs. Pandas UDF vs. Python UDF

# COMMAND ----------

# Create a DataFrame with 10 million rows
number_of_rows = 10_000_000

df = (
  spark
  .range(0, number_of_rows)
  .withColumn('v', F.rand())
)

# Cache the DataFrame to avoid recomputing
df.cache()

# Trigger the computation and caching
df.count()

# COMMAND ----------

display(df)

# COMMAND ----------

# DBTITLE 1,Define UDFs
@F.udf(T.DoubleType())
def plus_one(v):
    return v + 1

@F.pandas_udf(T.DoubleType())
def pandas_plus_one(v):
    return v + 1

def standard_sql(df, col_name):
    return df.withColumn(col_name, F.col(col_name) + 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time actually running the various UDFs
# MAGIC
# MAGIC ![](https://www.databricks.com/sites/default/files/2023-05/image1-4-og.png)
# MAGIC
# MAGIC Source: https://www.databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html

# COMMAND ----------

# DBTITLE 1,Standard SQL
# MAGIC %timeit standard_sql(df, 'v').agg(F.count(F.col('v'))).collect()

# COMMAND ----------

# DBTITLE 1,Pandas UDF
# MAGIC %timeit df.withColumn('v', pandas_plus_one(df.v)).agg(F.count(F.col('v'))).collect()

# COMMAND ----------

# DBTITLE 1,Python UDF
# MAGIC %timeit df.withColumn('v', plus_one(df.v)).agg(F.count(F.col('v'))).collect()
