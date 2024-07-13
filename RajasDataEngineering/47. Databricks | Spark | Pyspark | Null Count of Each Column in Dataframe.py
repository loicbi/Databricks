# Databricks notebook source
# MAGIC %md
# MAGIC ### Read dataframe 

# COMMAND ----------

# Create DataFrames from sample data
# Create a list of dictionaries containing employee data
employees = [
    {'Name': 'John Doe', 'Age': 30, 'Department': 'Marketing'},
    {'Name': 'Jane', 'Age': 25, 'Department': 'Sales'},
    {'Name': 'Bob Johnson', 'Age': 40, 'Department': None},
    {'Name': 'Loic', 'Age': 40, 'Department': 'Engineering'},
    {'Name': 'Assogba', 'Age': None, 'Department': None},
    {'Name': 'AWS', 'Age': None, 'Department': 'Engineering'},
    {'Name': None, 'Age': None, 'Department': None},
]

df = spark.createDataFrame(data=employees)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Find Null Occurences of Each Column in Dataframe  

# COMMAND ----------

from pyspark.sql.functions import col, when, count

result_null = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])

result_null.display()

# COMMAND ----------


