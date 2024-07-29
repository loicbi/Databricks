# Databricks notebook source
# MAGIC %md
# MAGIC Why and How: Partitioning in Databricks
# MAGIC
# MAGIC
# MAGIC https://medium.com/@eduard2popa/why-and-how-partitioning-in-databricks-e9e6f960db43

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read file Input Dataset   -   Create Sample dataset 

# COMMAND ----------

df = spark.read.format('csv').option('inferSchema', True).option('header', True).option('sep', ',').load('/FileStore/tables/amazon_input/amazon_prime_user.csv')
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get default partition Count  

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC Get number row count df 

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get numbers of records by partition 

# COMMAND ----------

from pyspark.sql.functions import spark_partition_id

df_partition_by_row_count = df.withColumn("PartionId", spark_partition_id()).groupBy('PartionId').count()
display(df_partition_by_row_count)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Repartition the Dataframe to 5 / Set to 5  (df.repartition(n))

# COMMAND ----------

df_5 = df.select(df['User ID'], df['Name'], df['Email Address']).repartition(5)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check number partition 

# COMMAND ----------

df_5.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get Number of Records per Partition 

# COMMAND ----------

df_5_count_partition = df_5.withColumn('Partition_Id', spark_partition_id()).groupBy('Partition_Id').count()
display(df_5_count_partition)

# COMMAND ----------


