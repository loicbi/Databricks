# Databricks notebook source
# MAGIC %md
# MAGIC CREATE SCHEMA OUR SAMPLE STREAMING DATA 

# COMMAND ----------

from pyspark.sql.types import * 

schema_defined = StructType(
    [
        StructField('User ID', IntegerType()),
        StructField('Name', StringType()),
        StructField('Email Address', StringType()),
        StructField('Username', StringType()),
        StructField('Date of Birth', TimestampType()),
        StructField('Gender', StringType()),
        StructField('Location', StringType()),
        StructField('Membership Start Date', TimestampType()),
        StructField('Membership End Date', TimestampType()),
        StructField('Subscription Plan', StringType()),
        StructField('Payment Information', StringType()),
        StructField('Renewal Status', StringType()),
        StructField('Usage Frequency', StringType()),
        StructField('Purchase History', StringType()),
        StructField('Favorite Genres', StringType()),
        StructField('Devices Used', StringType()),
        StructField('Engagement Metrics', StringType()),
        StructField('Feedback/Ratings', IntegerType()),
        StructField('Customer Support Interactions', IntegerType())
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC CREATE FOLDER STRUCTURE IN DBFS FILE SYSTEM 

# COMMAND ----------

# mkdirs : create folder 
dbutils.fs.mkdirs("/FileStore/tables/stream_read/")
dbutils.fs.mkdirs("/FileStore/tables/stream_checkpoint/")
dbutils.fs.mkdirs("/FileStore/tables/stream_write/")

# rm: remove 
# dbutils.fs.rm("/FileStore/tables/stream_checkpoint/", True)
# dbutils.fs.rm("/FileStore/tables/stream_read/", True)
# dbutils.fs.rm("/FileStore/tables/stream_write/", True)


# COMMAND ----------

df_read_infer_schema = spark.read.format('csv').option('inferSchema', True).option('header', True).option('sep', ',').load('/FileStore/tables/stream_read/')
display(df_read_infer_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC READ STREAMING DATA

# COMMAND ----------

df = spark.readStream.format('csv').schema(schema_defined).option('header', True).option('sep', ',').load('/FileStore/tables/stream_read/')
df_read = df.groupBy('Gender').sum('User ID')
display(df_read)

# COMMAND ----------

# MAGIC %md
# MAGIC WRITE STREAMING DATA

# COMMAND ----------

# # mkdirs : create folder 
# dbutils.fs.mkdirs("/FileStore/tables/stream_read/")
# dbutils.fs.mkdirs("/FileStore/tables/stream_checkpoint/")
# dbutils.fs.mkdirs("/FileStore/tables/stream_write/")

# rm: remove 
# dbutils.fs.rm("/FileStore/tables/stream_checkpoint/", True)
# dbutils.fs.rm("/FileStore/tables/stream_read/", True)
# dbutils.fs.rm("/FileStore/tables/stream_write/", True)

df_write = df.writeStream.format('parquet').outputMode('append').option('path', '/FileStore/tables/stream_write/').option('checkpointLocation', '/FileStore/tables/stream_checkpoint/').start().awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC VERIFY THE WRITTEN STREAM OUTPUT DATA 

# COMMAND ----------

display(spark.read.format('parquet').load('/FileStore/tables/stream_write/*.parquet'))

# COMMAND ----------


