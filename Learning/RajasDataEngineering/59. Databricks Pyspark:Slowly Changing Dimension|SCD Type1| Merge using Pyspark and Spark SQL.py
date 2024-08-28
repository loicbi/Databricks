# Databricks notebook source
# MAGIC %md
# MAGIC https://medium.com/analytics-vidhya/scd-type1-implementation-in-pyspark-f3ded001fec8

# COMMAND ----------

dbutils.fs.rm('/FileStore/tables/delta_merge', True)

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP TABLE IF EXISTS default.dim_employee;

# COMMAND ----------

from pyspark.sql.functions import * 
from pyspark.sql.types import * 

schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("contact_no", IntegerType(), True),
])

# COMMAND ----------

data = [(1000, "Toto", "Montreal", "CANADA", 123456789)]
df = spark.createDataFrame(data=data, schema=schema)
display(df)

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE default.dim_employee(
# MAGIC   EMP_ID INT,
# MAGIC   NAME STRING,
# MAGIC   CITY STRING,
# MAGIC   COUNTRY STRING,
# MAGIC   CONTACT_NO STRING
# MAGIC )
# MAGIC USING DELTA 
# MAGIC LOCATION '/FileStore/tables/delta_merge'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 1: Spark SQL

# COMMAND ----------

df.createOrReplaceTempView("source_view")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### source 

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM source_view;

# COMMAND ----------

# MAGIC %md
# MAGIC ###### destination: dim_employee 

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM default.dim_employee;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Script   ---  SCD 1

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO default.dim_employee as destination
# MAGIC USING source_view as source
# MAGIC ON destination.EMP_ID = source.emp_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     destination.EMP_ID = source.emp_id,
# MAGIC     destination.NAME = source.name,
# MAGIC     destination.CITY = source.city,
# MAGIC     destination.COUNTRY = source.country,
# MAGIC     destination.CONTACT_NO = source.contact_no
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     EMP_ID,
# MAGIC     NAME,
# MAGIC     CITY,
# MAGIC     COUNTRY,
# MAGIC     CONTACT_NO
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     source.emp_id,
# MAGIC     source.name,
# MAGIC     source.city,
# MAGIC     source.country,
# MAGIC     source.contact_no
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM default.dim_employee;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Insert Data in source view 

# COMMAND ----------

data = [(1000, "Toto", "Abidjan", "IVORY COAST", 123456789), (2000, "Tata", "Yamoussoukro", "IVORY COAST", 999999999), ]
df = spark.createDataFrame(data=data, schema=schema)
df.createOrReplaceTempView('source_view')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO default.dim_employee as destination
# MAGIC USING source_view as source
# MAGIC ON destination.EMP_ID = source.emp_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     destination.EMP_ID = source.emp_id,
# MAGIC     destination.NAME = source.name,
# MAGIC     destination.CITY = source.city,
# MAGIC     destination.COUNTRY = source.country,
# MAGIC     destination.CONTACT_NO = source.contact_no
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     EMP_ID,
# MAGIC     NAME,
# MAGIC     CITY,
# MAGIC     COUNTRY,
# MAGIC     CONTACT_NO
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     source.emp_id,
# MAGIC     source.name,
# MAGIC     source.city,
# MAGIC     source.country,
# MAGIC     source.contact_no
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM default.dim_employee;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pyspark -- SCD 1

# COMMAND ----------

# source 
data = [(1000, "tITI", "Abidjan", "IVORY COAST", 123456789), (3000, "Jean", "Yapokoi", "IVORY COAST", 78787878), ]
df_source = spark.createDataFrame(data=data, schema=schema)
display(df_source)

# COMMAND ----------

# destination : dim_employee
from delta.tables import *

df_instance_destination = DeltaTable.forPath(spark, '/FileStore/tables/delta_merge')



# COMMAND ----------

from delta.tables import *

dfUpdates_destination = df_instance_destination.toDF()

df_instance_destination.alias('target').merge(
    source = df_source.alias('source'),
    condition = 'source.emp_id = target.EMP_ID'
  ) \
  .whenMatchedUpdate(set =
    {
      "target.EMP_ID": "source.emp_id",
      "target.NAME": "source.name",
      "target.CITY": "source.city",
      "target.COUNTRY": "source.country",
      "target.CONTACT_NO": "source.contact_no"
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "target.EMP_ID": "source.emp_id",
      "target.NAME": "source.name",
      "target.CITY": "source.city",
      "target.COUNTRY": "source.country",
      "target.CONTACT_NO": "source.contact_no",
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM default.dim_employee;

# COMMAND ----------


