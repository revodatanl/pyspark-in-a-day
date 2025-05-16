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

# MAGIC %md
# MAGIC # Execution plan example -> Filtering

# COMMAND ----------

from pyspark.sql import functions as F

# Simple transformation: Select specific columns and filter rows
df_sales_transactions_filtered = df_sales_transactions.select("transactionID", "totalPrice").filter(F.col("totalPrice") > 50)

# Execute an explain on the transformed DataFrame
df_sales_transactions_filtered.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC | Stage                      | Purpose                                                                     | Key Characteristics                                                                                                           |
# MAGIC | :------------------------- | :-------------------------------------------------------------------------- | :---------------------------------------------------------------------------------------------------------------------------- |
# MAGIC | **Parsed Logical Plan**    | Converts your SQL / DataFrame API query into an abstract syntax tree (AST). | Table names and columns are **unresolved**. It’s just the **structure** of the query.                                         |
# MAGIC | **Analyzed Logical Plan**  | Validates the plan against the catalog and schema.                          | **Resolves table names and column references**, assigns internal IDs, infers data types, and checks for errors.                                  |
# MAGIC | **Optimized Logical Plan** | Applies rule-based optimizations on the logical plan.                       | Removes redundant operations, pushes filters down, prunes columns, simplifies expressions.                                    |
# MAGIC | **Physical Plan**          | Converts the logical plan into an executable physical plan.                 | Specifies **how** to execute the query — using scans, joins, filters, shuffles, etc., including Photon or Tungsten execution. |
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

# Simple transformation: Select specific columns and filter rows
df_sales_transactions_filtered = (
    df_sales_transactions
    .select("transactionID", "totalPrice")
    .filter(F.col("totalPrice") > 50)
    .filter(F.col("totalPrice") > 25)
)

# Execute an explain on the transformed DataFrame
df_sales_transactions_filtered.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC # Shuffle operations
# MAGIC
# MAGIC A network-intensive operation where data gets redistributed (partitioned) across the cluster.
# MAGIC - E.g., based on a key, like `customerID`
# MAGIC
# MAGIC Shuffles are expensive
# MAGIC - Network IO: Data moves between executors.
# MAGIC - Disk IO: Intermediate data is written and read during the shuffle.
# MAGIC - Serialization overhead: Data gets serialized and deserialized.

# COMMAND ----------

df_joined = (
    df_sales_transactions
    .join(df_sales_customers, how="left", on="customerID")
    .join(df_sales_franchises, how="left", on="franchiseID")
    .select("product")
    .distinct()
)

# COMMAND ----------

df_joined.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC The execution plan states:
# MAGIC
# MAGIC ```
# MAGIC PhotonShuffleExchangeSink hashpartitioning(product#10769, 1024)
# MAGIC ```
# MAGIC
# MAGIC - Spark partitions the data across 1024 partitions based on the product column.
# MAGIC - This is because to perform the deduplication (essentially a group by) on product, all rows with the same product value must end up in the same partition to safely deduplicate them.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC In this case:
# MAGIC - The shuffle is necessary for correctness because deduplication (or grouping) requires co-locating identical values.
# MAGIC - The number of partitions (1024) affects performance:
# MAGIC   - Too many = overhead in task scheduling and small files.
# MAGIC   - Too few = skewed, large partitions → memory pressure and slow processing.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Optimizations
# MAGIC - Reduce the number of shuffle partitions if your data volume doesn’t justify 1024:
# MAGIC   `spark.conf.set("spark.sql.shuffle.partitions", "200")`
# MAGIC - Pre-partition your data by product if you do this kind of aggregation often.
# MAGIC - Check for skew — if some products are overly common, you may get skewed partitions
# MAGIC   - → Need to handle skewed joins or aggregations (e.g., salting techniques).

# COMMAND ----------

from pyspark.sql import functions as F

# 1. Group by franchiseID and product to get total quantity and revenue per product per franchise
df_grouped = (
    df_sales_transactions.groupBy("franchiseID", "product")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.sum("totalPrice").alias("total_revenue"),
        F.avg("unitPrice").alias("avg_unit_price")
    )
)

# 2. Pivot the product column to see revenue per franchise per product
df_pivoted = (
    df_sales_transactions
    .groupBy("franchiseID")
    .pivot("product")
    .sum("totalPrice")
)

# 3. Aggregate sales by month and payment method
df_monthly_sales = (
    df_sales_transactions
    .withColumn("month", F.date_format(F.col("dateTime"), "yyyy-MM"))
    .groupBy("month", "paymentMethod")
    .agg(
        F.sum("totalPrice").alias("total_revenue"),
        F.count("transactionID").alias("total_transactions")
    )
)

# 4. Find the top-selling product per franchise (using window functions)
from pyspark.sql.window import Window

window_spec = Window.partitionBy("franchiseID").orderBy(F.col("total_quantity").desc())

df_top_products = (
    df_grouped
    .withColumn("rank", F.row_number().over(window_spec))
    .filter(F.col("rank") == 1)
    .drop("rank")
)

# 5. Join with franchise data (assuming df_franchises contains franchise info)
df_sales_with_franchise = df_grouped.join(df_sales_franchises, "franchiseID", "left")

# COMMAND ----------

df_sales_with_franchise.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Scan `sales_transaction`
# MAGIC
# MAGIC ```
# MAGIC PhotonScan parquet samples.bakehouse.sales_transactions[…]
# MAGIC ```
# MAGIC
# MAGIC - Spark reads necessary columns (franchiseID, product, quantity, unitPrice, totalPrice) from the Parquet data source.
# MAGIC - No filters applied here.
# MAGIC
# MAGIC ## Partial Aggregation (Map-side)
# MAGIC
# MAGIC ```
# MAGIC PhotonGroupingAgg(keys=[franchiseID, product], functions=[partial_sum, partial_avg], …)
# MAGIC ```
# MAGIC
# MAGIC - Spark performs a partial aggregation locally on each executor.
# MAGIC - Computes intermediate sums and counts for quantity, totalPrice, unitPrice.
# MAGIC - Reduces the volume of data sent over the network in the next shuffle.
# MAGIC
# MAGIC ## Shuffle Exchange (Hash Partitioning)
# MAGIC
# MAGIC ```
# MAGIC PhotonShuffleExchangeSink hashpartitioning(franchiseID, product, 1024)
# MAGIC ```
# MAGIC
# MAGIC This is an expensive step. Network + disk IO happens here.
# MAGIC
# MAGIC - Data is shuffled across the cluster by `franchiseID` and `product`.
# MAGIC - Ensures all records for the same `franchiseID` & `product` land in the same partition for the next aggregation step.
# MAGIC
# MAGIC ## Final Aggregation (Reduce-side)
# MAGIC
# MAGIC ```
# MAGIC PhotonGroupingAgg(keys=[franchiseID, product], functions=[finalmerge_sum, finalmerge_avg], …)
# MAGIC ```
# MAGIC
# MAGIC - Merges the partial aggregates from each partition.
# MAGIC - Computes final `sum(quantity)`, `sum(totalPrice)`, and `avg(unitPrice)`.
# MAGIC
# MAGIC ## Broadcast the Franchise Dimension
# MAGIC
# MAGIC ```
# MAGIC PhotonShuffleExchangeSink SinglePartition
# MAGIC ```
# MAGIC
# MAGIC - The `sales_franchises` table is collected into a single partition (small dimension table).
# MAGIC - Broadcasted to all executors for the join.
# MAGIC - The PhotonBroadcastHashJoin hints Spark that the right side (franchises) is small enough to broadcast.
# MAGIC - Great for performance as it avoids a costly shuffle join.
# MAGIC
# MAGIC ## Join
# MAGIC
# MAGIC ```
# MAGIC PhotonBroadcastHashJoin [franchiseID], [franchiseID], LeftOuter, BuildRight
# MAGIC ```
# MAGIC
# MAGIC - Performs a left outer join between:
# MAGIC   - the aggregated sales data
# MAGIC   - and the broadcasted franchise data.
# MAGIC - `BuildRight` means the `sales_franchises` table is broadcasted.
