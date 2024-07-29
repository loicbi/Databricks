# Databricks notebook source
# MAGIC %md
# MAGIC ## Snappy vs Gzip 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read file csv 

# COMMAND ----------

csvFile = '/FileStore/tables/stream_read/amazon_prime_users_1.csv'

csvDF = spark.read.format('csv').option('inferSchema', True).option('sep', ',').option('header', True).load(csvFile)
csvDF.display()

# COMMAND ----------

# MAGIC %md
# MAGIC https://medium.com/@ARishi/what-are-the-different-write-modes-in-spark-d3518e96be23#:~:text=The%20available%20write%20modes%20are,exist%2C%20it%20will%20be%20created.&text=append%3A%20This%20mode%20appends%20the,existing%20data%20in%20the%20file.
# MAGIC
# MAGIC
# MAGIC overwrite: This mode overwrites any existing data in the file. If the file does not exist, it will be created.
# MAGIC
# MAGIC append: This mode appends the data to the file, preserving any existing data in the file. If the file does not exist, it will be created.
# MAGIC
# MAGIC ignore: This mode writes the data to the file only if the file does not already exist. If the file already exists, the write operation is ignored.
# MAGIC
# MAGIC error: This mode raises an error if the file already exists.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### parquet-snappy

# COMMAND ----------

# convert df to parquet-snappy 
csvDF.write.format('parquet').mode('append').option('compression', 'snappy').save('/FileStore/tables/write_files/snappy_parquets')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/write_files/snappy_parquets

# COMMAND ----------

# MAGIC %md
# MAGIC #### parquet-gzip

# COMMAND ----------

# convert df to parquet-snappy 
csvDF.write.format('parquet').mode('append').option('compression', 'gzip').save('/FileStore/tables/write_files/gzip_parquets')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/write_files/gzip_parquets/

# COMMAND ----------

dbutils.fs.rm('dbfs:/FileStore/tables/write_files',True)

# COMMAND ----------


