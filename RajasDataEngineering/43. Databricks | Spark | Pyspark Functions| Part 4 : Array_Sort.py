# Databricks notebook source
# MAGIC %md
# MAGIC ### Create simple data 

# COMMAND ----------

from pyspark.sql.functions import *
data = [
    ([1, 2, 3], [2, 4, 6]),\
    ([7, 6, 9], [6, 11, 7]),\
    ([2, 6, 15], [6, 17, 6]),\
]
df = spark.createDataFrame(data, ['vals1', 'vals2'])
df.show(10, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Array Sort

# COMMAND ----------

df_sorted = df.withColumn('Sorted', array_sort(df.vals1)).drop('vals2')

display(df_sorted)

# COMMAND ----------


