# Databricks notebook source
# MAGIC %md
# MAGIC ### Method 1: Pyspark

# COMMAND ----------

dbutils.fs.rm('/FileStore/tables/delta/create_table', True)
dbutils.fs.rm('/tmp/delta/people10m', True)

# COMMAND ----------

from delta.tables import *

# Create table in the metastore
DeltaTable.createOrReplace(spark) \
  .tableName("default.employee_demo") \
  .addColumn("id", "INT") \
  .addColumn("firstName", "STRING") \
  .addColumn("middleName", "STRING") \
  .addColumn("lastName", "STRING", comment = "surname") \
  .addColumn("gender", "STRING") \
  .addColumn("birthDate", "TIMESTAMP") \
  .addColumn("ssn", "STRING") \
  .addColumn("salary", "INT") \
  .property("description", "table with people data") \
  .location("/FileStore/tables/delta/create_table") \
  .execute()

# Create or replace table with path and add properties
DeltaTable.createIfNotExists(spark) \
  .tableName('default.employee_demo')
  .addColumn("id", "INT") \
  .addColumn("firstName", "STRING") \
  .addColumn("middleName", "STRING") \
  .addColumn("lastName", "STRING", comment = "surname") \
  .addColumn("gender", "STRING") \
  .addColumn("birthDate", "TIMESTAMP") \
  .addColumn("ssn", "STRING") \
  .addColumn("salary", "INT") \
  .property("description", "table with people data") \
  .location("/FileStore/tables/delta/create_table") \
  .execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 2: SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS default.employee_demo (
# MAGIC   id INT,
# MAGIC   firstName STRING,
# MAGIC   middleName STRING,
# MAGIC   lastName STRING,
# MAGIC   gender STRING,
# MAGIC   birthDate TIMESTAMP,
# MAGIC   ssn STRING,
# MAGIC   salary INT
# MAGIC ) USING DELTA
# MAGIC LOCATION '/FileStore/tables/delta/create_table';
# MAGIC
# MAGIC CREATE OR REPLACE TABLE default.employee_demo (
# MAGIC   id INT,
# MAGIC   firstName STRING,
# MAGIC   middleName STRING,
# MAGIC   lastName STRING,
# MAGIC   gender STRING,
# MAGIC   birthDate TIMESTAMP,
# MAGIC   ssn STRING,
# MAGIC   salary INT
# MAGIC ) USING DELTA
# MAGIC LOCATION '/FileStore/tables/delta/create_table';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 3: Using DataFrame
# MAGIC DataFrameWriter API: If you want to simultaneously create a table and insert data into it from Spark DataFrames or Datasets, you can use the Spark DataFrameWriter (Scala or Java and Python).
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS default.employee_demo;

# COMMAND ----------

# remove path file location 
dbutils.fs.rm('/FileStore/tables/delta/create_table', True)
dbutils.fs.rm('/tmp', True)


# COMMAND ----------

# create df 
employee_data = [
    (100, "Stephen", "M", 2000, "IT"),
    (200, "Philipp", "M", 2000, "HR"),
    (300, "Lara", "F", 6000, "SALES"),
]

employee_schema = ['employee_id', 'emp_name', 'gender', 'salaray', 'dept']
df = spark.createDataFrame(data = employee_data, schema=employee_schema)
df.show(truncate=False)

# COMMAND ----------

# Create table in the metastore using DataFrame's schema and write data to it
df.write.format("delta").mode("overwrite").saveAsTable("default.employee_demo")
df.show(truncate=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM default.employee_demo;

# COMMAND ----------


# Create or replace partitioned table with path using DataFrame's schema and write/overwrite data to it
df.write.format("delta").mode("overwrite").save("/FileStore/tables/delta/create_table")

# COMMAND ----------

from delta.tables import * 

df2 = DeltaTable.forPath(spark, '/FileStore/tables/delta/create_table')

display(df2.toDF())

# COMMAND ----------

df2.toDF().createOrReplaceTempView('temp_view')

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- SELECT * FROM delta.`/FileStore/tables/delta/create_table`;
# MAGIC -- OR 
# MAGIC SELECT  *   FROM temp_view limit 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from  default.employee_demo where 
# MAGIC employee_id = 100;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY default.employee_demo;

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE TABLE FORMATTED default.employee_demo;

# COMMAND ----------


