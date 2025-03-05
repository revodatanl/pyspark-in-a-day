# Databricks notebook source
# MAGIC % pip install faker

# COMMAND ----------

# MAGIC %md
# MAGIC # **Supported Data Sources and Integration in Spark**
# MAGIC
# MAGIC Spark SQL supports a diverse range of **data sources**, enabling seamless integration with **file-based storage, relational databases, data warehouses, and serialization formats**. These sources are accessed through the **DataFrame API**, allowing relational transformations and SQL queries with built-in optimizations.
# MAGIC
# MAGIC ## **Supported Data Sources and Integrations**
# MAGIC - **File-Based Storage** – Supports **Parquet, ORC, JSON, CSV, Text, and Whole Binary Files**, featuring **schema merging, partition discovery, and recursive file lookup**.
# MAGIC - **Databases (JDBC)** – Connects to relational databases (**MySQL, PostgreSQL, SQL Server, Oracle**) via **JDBC**, supporting **save modes and persistent table options**.
# MAGIC
# MAGIC ## **Optimization & Configuration Features**
# MAGIC - **Bucketing, Sorting, and Partitioning** – Enhances query performance.
# MAGIC - **Save Modes** – Controls data persistence and overwriting behavior.
# MAGIC - **Handling Corrupt/Missing Files** – Improves fault tolerance in data loading.
# MAGIC - **Path Global Filters & Recursive File Lookup** – Automates file discovery and selection.
# MAGIC
# MAGIC ## Custom connectors
# MAGIC - A PySpark DataSource is created by the Python (PySpark) DataSource API, which enables reading from custom data sources and writing to custom data sinks in Apache Spark using Python. You can use PySpark custom data sources to define custom connections to data systems and implement additional functionality, to build out reusable data sources.
# MAGIC
# MAGIC ## Sources
# MAGIC - https://spark.apache.org/docs/3.5.3/sql-data-sources.html 
# MAGIC - https://docs.databricks.com/aws/en/pyspark/datasources
# MAGIC - https://docs.databricks.com/aws/en/pyspark/basics

# COMMAND ----------

# import all
from pyspark.sql.types import *
from pyspark.sql.functions import *

import pyspark.sql.types as T
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading from delta

# COMMAND ----------

df_customer = spark.read.table('samples.tpch.customer')
display(df_customer)

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading from CSV
# MAGIC
# MAGIC ```
# MAGIC volume_file_path = "/Volumes/path"
# MAGIC
# MAGIC df_csv = (spark.read
# MAGIC   .format("csv")
# MAGIC   .option("header", True)
# MAGIC   .option("inferSchema", True)
# MAGIC   .load(volume_file_path)
# MAGIC )
# MAGIC display(df_csv)
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC # Reading from JSON 
# MAGIC
# MAGIC ```
# MAGIC volume_file_path = "/Volumes/path"
# MAGIC df = spark.read
# MAGIC   .format("json") 
# MAGIC   .load(volume_file_path)
# MAGIC ```
# MAGIC
# MAGIC # Etc....

# COMMAND ----------

# MAGIC %md
# MAGIC # Custom connector

# COMMAND ----------

from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import StructType

class FakeDataSourceReader(DataSourceReader):

    def __init__(self, schema, options):
        self.schema: StructType = schema
        self.options = options

    def read(self, partition):
        # Library imports must be within the method.
        from faker import Faker
        fake = Faker()

        # Every value in this `self.options` dictionary is a string.
        num_rows = int(self.options.get("numRows", 3))
        for _ in range(num_rows):
            row = []
            for field in self.schema.fields:
                value = getattr(fake, field.name)()
                row.append(value)
            yield tuple(row)

class FakeDataSource(DataSource):
    """
    An example data source for batch query using the `faker` library.
    """

    @classmethod
    def name(cls):
        return "fake"

    def schema(self):
        return "name string, date string, zipcode string, state string"

    def reader(self, schema: StructType):
        return FakeDataSourceReader(schema, self.options)

spark.dataSource.register(FakeDataSource)
spark.read.format("fake").load().show()