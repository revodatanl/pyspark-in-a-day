# Databricks notebook source
# MAGIC %md
# MAGIC - **PySpark Built-in Methods**:
# MAGIC   - Highly optimized for distributed computing
# MAGIC   - Fast execution and scalability
# MAGIC   - Efficient resource utilization
# MAGIC   - Recommended for best performance
# MAGIC
# MAGIC - **Pandas-on-Spark**:
# MAGIC   - User-friendly, familiar pandas-like syntax
# MAGIC   - Simplifies transition from pandas workflows
# MAGIC   - Slightly less performant due to abstraction overhead
# MAGIC   - Convenient for data scientists familiar with pandas
# MAGIC
# MAGIC - **Python UDFs**:
# MAGIC   - High flexibility for custom computations
# MAGIC   - Easier to implement complex logic
# MAGIC   - Typically slower due to serialization overhead
# MAGIC   - Recommended when built-in methods or APIs do not suffice
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # User-Defined functions (UDF's)
# MAGIC User-Defined Functions (UDFs) in PySpark allow you to define custom functions and apply them to DataFrame columns. They are useful when built-in Spark functions do not provide the needed functionality.

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.types as T

# COMMAND ----------

# Sample DataFrame -> Generate our own data
data = [(1,), (2,), (3,), (4,)]
df = spark.createDataFrame(data, ["number"])
display(df)


# COMMAND ----------

# Define a Python function
def square(x):
    return x * x
    
# Convert it into a UDF
square_udf = udf(square, T.IntegerType())

# Apply UDF to DataFrame
df = df.withColumn("squared", square_udf(df["number"]))

# Show results
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Lets try it out on the sample data

# COMMAND ----------

# Read from a catalog
df_sales_transactions = spark.read.table("samples.bakehouse.sales_transactions")

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Define a Python function
def concatenate_columns(col1, col2):
    return col1 + col2

# Convert it into a UDF
concat_udf = udf(concatenate_columns, StringType())

# Apply UDF to DataFrame
df_sales_transactions = df_sales_transactions.withColumn("concatenated", concat_udf(df_sales_transactions["product"], df_sales_transactions["paymentMethod"]))

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Define a Python function
def concatenate_columns(col1, col2):
    return col1 + col2

# Convert it into a UDF
concat_udf = udf(concatenate_columns, StringType())

# Apply UDF to DataFrame
df_sales_transactions = df_sales_transactions.withColumn("concatenated", concat_udf(df_sales_transactions["product"], df_sales_transactions["paymentMethod"]))

display(df_sales_transactions)

# COMMAND ----------

# MAGIC %md
# MAGIC # Pandas on Spark UDF's
# MAGIC
# MAGIC ![Pandas on Spark UDF's](https://www.databricks.com/sites/default/files/2023-05/image1-4-og.png)

# COMMAND ----------

from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType

# Define a Python function
def concatenate_columns(col1, col2):
    return col1 + col2

# Convert it into a pandas UDF
concat_udf = pandas_udf(concatenate_columns, StringType())

# Apply UDF to DataFrame
df_sales_transactions = df_sales_transactions.withColumn("concatenated", concat_udf(df_sales_transactions["product"], df_sales_transactions["paymentMethod"]))

display(df_sales_transactions)

# COMMAND ----------

# MAGIC %md
# MAGIC # Built in spark

# COMMAND ----------

from pyspark.sql.functions import concat

# Apply built-in Spark concat function to DataFrame
df_sales_transactions = df_sales_transactions.withColumn("concatenated", concat(df_sales_transactions["product"], df_sales_transactions["paymentMethod"]))

display(df_sales_transactions)

# COMMAND ----------

# MAGIC %md
# MAGIC # Performance differences for SQRT method

# COMMAND ----------

import time
from pyspark.sql.functions import udf, pandas_udf,sqrt
from pyspark.sql.types import IntegerType, StringType, DoubleType
import pandas as pd

# Generate dataset (millions of rows)
NR_ROWS = 10_000_000
data = [(i,) for i in range(NR_ROWS)]

# COMMAND ----------

df = spark.createDataFrame(data, ["number"])
df = df.repartition(4)
df.cache()
df.count()

# COMMAND ----------

# --- Built-in Spark SQL Expression (Fastest) ---
from pyspark.sql.functions import sqrt

start = time.time()
df_1 = df.withColumn("squared_builtin", sqrt("number"))
df_1.count()  # Trigger action
print("Built-in Spark SQL Time:", time.time() - start)

# COMMAND ----------

# --- Pandas UDF (Faster) ---
@pandas_udf(DoubleType())
def sqrt_pandas(x: pd.Series) -> pd.Series:
    return x^0.5

start = time.time()
df_2 = df.withColumn("sqrt_pandas", sqrt_pandas(df["number"]))
df_2.count()  # Trigger action
print("Pandas UDF Time:", time.time() - start)


# COMMAND ----------

# --- Standard Python UDF (Slowest) ---
def sqrt_python(x):
    return x^0.5

square_udf = udf(sqrt_python, DoubleType())

start = time.time()
df_3 = df.withColumn("squared_python", square_udf(df["number"]))
df_3.count()  # Trigger action
print("Python UDF Time:", time.time() - start)

# COMMAND ----------

