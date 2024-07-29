# Databricks notebook source
# MAGIC %md
# MAGIC ###  Read CSV file without Any Option 

# COMMAND ----------

df = spark.read.format('csv').load('/FileStore/tables/amazon_input/amazon_prime_user.csv')
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Read CSV file with Infer Schema True

# COMMAND ----------

df = spark.read.format('csv').option('inferSchema', True).load('/FileStore/tables/amazon_input/amazon_prime_user.csv')
display(df)

# COMMAND ----------


