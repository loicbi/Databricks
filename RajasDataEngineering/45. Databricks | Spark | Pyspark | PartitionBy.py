# Databricks notebook source
# MAGIC %md
# MAGIC ## Generic Load/Save Functions
# MAGIC https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read file Input Dataset   -   Create Sample dataset 

# COMMAND ----------

dbutils.fs.rm('/FileStore/tables/amazon_input/', True)
dbutils.fs.rm('/FileStore/tables/amazon_output/', True)
dbutils.fs.cp('/FileStore/tables/stream_read/amazon_prime_users_1.csv', '/FileStore/tables/amazon_input/amazon_prime_user.csv')

# COMMAND ----------

# copy file 
# dbutils.fs.cp('/FileStore/tables/stream_read/amazon_prime_users_1.csv', '/FileStore/tables/amazon_input/amazon_prime_user.csv')

# delete 
# dbutils.fs.rm('/FileStore/tables/amazon_input/', True)
# dbutils.fs.rm('/FileStore/tables/amazon_output/', True)

df = spark.read.format('csv').option('inferSchema', True).option('header', True).option('sep', ',').load('/FileStore/tables/amazon_input/amazon_prime_user.csv')
display(df)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/amazon_input/
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Distinct Year List and Count foreach Year

# COMMAND ----------

df.groupBy('Payment Information').count().show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Partion By one key column 

# COMMAND ----------

df.write.option('header', True)\
  .partitionBy('Payment Information')\
  .mode('overwrite')\
  .csv('/FileStore/tables/amazon_output/one_partition')


# COMMAND ----------

# MAGIC %md
# MAGIC ### Partion By multiple keys columns

# COMMAND ----------

# remove path 
dbutils.fs.rm('/FileStore/tables/amazon_output/', True)

# add path 
dbutils.fs.mkdirs('/FileStore/tables/amazon_output/')

# COMMAND ----------

df.write.option('header', True)\
  .partitionBy('Payment Information','Gender')\
  .mode('overwrite')\
  .csv('/FileStore/tables/amazon_output/many_partition') 

# COMMAND ----------

display(df)

# COMMAND ----------


