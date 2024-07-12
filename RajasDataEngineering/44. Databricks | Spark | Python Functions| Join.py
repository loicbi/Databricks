# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC ## Read file 

# COMMAND ----------

df = spark.read.format('csv').option('inferSchema', True).option('header', True).option('sep', ',').load('/FileStore/tables/stream_read/')
df = df.select(df.columns[0:9])
display(df)

# COMMAND ----------

columnsList = df.columns
print(columnsList)
print(list(columnsList))
columnsString = ','.join(columnsList)
print(columnsString)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use Case

# COMMAND ----------

joinString = ' AND '.join(list(map(lambda x: (f'Target.{x}=Source.{x}'), columnList)))
print(joinString)

# COMMAND ----------

updateString = ', '.join(list(map(lambda x: (f'Target.{x}=Source.{x}'), columnList)))
print(updateString)

# COMMAND ----------

sourceInsertString = ','.join(list(map(lambda x: (f"Source.{x}"), columnList)))
print(sourceInsertString)

# COMMAND ----------


