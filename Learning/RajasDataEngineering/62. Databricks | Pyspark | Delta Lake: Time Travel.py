# Databricks notebook source
# MAGIC %sql 
# MAGIC SELECT * FROM default.scd2demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Time Travel 

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- TABLE HISTORY 
# MAGIC DESCRIBE HISTORY default.scd2demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pyspark Approaches 

# COMMAND ----------

# MAGIC %md
# MAGIC Method 1: Pyspark - TimeStamp + Table 

# COMMAND ----------

df = spark.read\
  .format('delta')\
    .option('timestampAsOf', '2024-06-14T19:55:30Z')\
      .table('default.scd2demo')
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Method 2: Pyspark - TimeStamp + Path 

# COMMAND ----------

df = spark.read\
  .format('delta')\
    .option('timestampAsOf', '2024-06-14T19:55:30Z')\
      .load('/FileStore/tables/scd2Demo')
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Method 3: Pyspark - Version + Path

# COMMAND ----------

df = spark\
    .read\
        .format('delta')\
            .option('versionAsOf', '1')\
                .load('/FileStore/tables/scd2Demo')
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Method 4: Pyspark - Version + Path

# COMMAND ----------

df = spark.read\
  .format('delta')\
    .option('versionAsOf', '4')\
      .table('default.scd2Demo')
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL Approaches
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Method 5: SQL - Version + Table

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM default.scd2demo VERSION AS OF 1;

# COMMAND ----------

# MAGIC %md
# MAGIC Method 6: SQL - Version + Path

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM delta.`/FileStore/tables/scd2Demo` VERSION AS OF 4;

# COMMAND ----------

# MAGIC %md
# MAGIC Method 7: SQL - Timestamp + Table

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM delta.`/FileStore/tables/scd2Demo` TIMESTAMP AS OF '2024-06-14T19:55:30Z';
