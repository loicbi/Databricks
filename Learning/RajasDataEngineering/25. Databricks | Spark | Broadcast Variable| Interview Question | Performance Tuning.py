# Databricks notebook source
# MAGIC %md
# MAGIC https://www.educative.io/answers/what-is-pyspark-broadcast-join
# MAGIC ## What is broadcast join, how to perform broadcast in pyspark ?
# MAGIC
# MAGIC PySpark broadcast join is a method used in PySpark (a Python library for Apache Spark) to improve joint operation performance when one of the joined tables is tiny. The primary goal of a broadcast join is to eliminate data shuffling and network overhead associated with join operations, which can result in considerable speed benefits. A broadcast join sends the smaller table (or DataFrame) to all worker nodes, ensuring each worker node has a complete copy of the smaller table in memory. This allows the join operation to be conducted locally on each worker node, eliminating the network's data shuffle and transfer requirement.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------


# Create simple dimension table 
sales_data = [(1, 101, 2), (2, 102, 1), (3, 103, 3), (4, 101, 1), (5, 104, 4)]
sales_columns = ["order_id", "product_id", "quantity"]
sales_df = spark.createDataFrame(sales_data, schema=sales_columns)


# COMMAND ----------

# Create DataFrames from sample data
products_data = [(101, "Learn Python", 10), (102, "Mobile: X1", 20), (103, "LCD", 30), (104, "Laptop", 40)]
products_columns = ["product_id", "product_name", "price"]
products_df = spark.createDataFrame(products_data, schema=products_columns)

# COMMAND ----------

from pyspark.sql.functions import broadcast
# Perform broadcast join
joinDf = sales_df.join(broadcast(products_df), sales_df['product_id'] == products_df['product_id'])

# Show result
display(joinDf)

# COMMAND ----------

joinDf.explain()

# COMMAND ----------


