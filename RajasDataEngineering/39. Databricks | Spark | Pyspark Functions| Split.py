# Databricks notebook source
# MAGIC %md
# MAGIC ### Read file csv 

# COMMAND ----------

from pyspark.sql.functions import  * 
csvFile = '/FileStore/tables/stream_read/amazon_prime_users_1.csv'

csvDF = spark.read.format('csv').option('inferSchema', True).option('sep', ',').option('header', True).load(csvFile)
df = csvDF.select('Name', 'Email Address', col('Membership Start Date').alias('Date'))

# COMMAND ----------

# split column Name 
df_split = df.withColumns({'First_Name':split(df['Name'], ' ').getItem(0),\
    'Last_Name':split(df['Name'], ' ').getItem(1), \
    'year': split(df['Date'], '-').getItem(0),
    'month': split(df['Date'], '-').getItem(1),
    'day': split(df['Date'], '-').getItem(2),
    })
df_split.display()

# COMMAND ----------


