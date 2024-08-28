# Databricks notebook source
# Create simple data 
from pyspark.sql.functions import *
data = [
    ([1, 2, 3], [2, 4, 6], ['aaaa', 'bbb', 888888888888]),\
    ([7, 6, 9], [6, 11, 7], ['cccc', 'ddd', 99999999999]),\
    ([2, 6, 15], [6, 17, 6], ['eee', 'ffff', 444444444444]),\
]
df = spark.createDataFrame(data, ['vals1', 'vals2', 'vals3'])
df.show(10, False)

# COMMAND ----------

# Apply Array_intersect 

df_intersect = df.withColumn("Intersect_Col", array_intersect(df.vals1, df.vals2))
df_intersect.display()

# COMMAND ----------


