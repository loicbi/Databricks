# Databricks notebook source
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

data = [(1000, "Toto", "Paris", "FRANCE", 123456789),(2000, "DADA", "Yamoussoukro", "IVORY COAST", 99999), (3000, "Jean", "Yapokoi", "IVORY COAST", 78787878),]
df = spark.createDataFrame(data=data, schema=schema)
display(df)

# COMMAND ----------

df.createOrReplaceTempView('source_view')

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM source_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM default.dim_employee;

# COMMAND ----------

data = [(2000, "Sephora", "Yamoussoukro", "IVORY COAST", 99999), (4000, "Fred", "Yapokoi", "IVORY COAST", 78787878),]
df = spark.createDataFrame(data=data, schema=schema)
display(df)
df.createOrReplaceTempView('source_view')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO default.dim_employee as target
# MAGIC USING source_view as source
# MAGIC   ON target.EMP_ID = source.emp_id
# MAGIC   WHEN MATCHED
# MAGIC THEN UPDATE SET
# MAGIC     target.EMP_ID = source.emp_id,
# MAGIC     target.NAME = source.name,
# MAGIC     target.CITY = source.city,
# MAGIC     target.COUNTRY = source.country,
# MAGIC     target.CONTACT_NO = source.contact_no
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC INSERT (
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

# MAGIC %md
# MAGIC ### Audit Log 

# COMMAND ----------

# MAGIC %sql 
# MAGIC --DROP TABLE IF EXISTS default.audit_log;
# MAGIC create table if not exists audit_log(
# MAGIC   operation string,
# MAGIC   update_time timestamp,
# MAGIC   user_name string,
# MAGIC   notebook_name string,
# MAGIC   numTargetRowsUpdated int,
# MAGIC   numTargetRowsInserted int,
# MAGIC   numTargetRowsDeleted int
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM default.audit_log;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Dataframe with last operation in Delta Table 

# COMMAND ----------

from delta.tables import *
delta_instance_df = DeltaTable.forPath(spark, '/FileStore/tables/delta_merge')

lastOperationDF = delta_instance_df.history(1) # argument 1 to get last history
display(lastOperationDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explode operaionMetrics Column 

# COMMAND ----------

explode_OperationMetric = lastOperationDF.select(lastOperationDF.userName,lastOperationDF.operation, explode(lastOperationDF.operationMetrics))
explode_select = explode_OperationMetric.select(explode_OperationMetric.userName,explode_OperationMetric.operation,explode_OperationMetric.key, explode_OperationMetric.value.cast('int'))
display(explode_select)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pivot Operation to convert Rows to Column

# COMMAND ----------

explode_OperationMetric = lastOperationDF.select(lastOperationDF.userName,lastOperationDF.operation, explode(lastOperationDF.operationMetrics))
explode_select = explode_OperationMetric.select(explode_OperationMetric.userName,explode_OperationMetric.operation,explode_OperationMetric.key, explode_OperationMetric.value.cast('int'))

display(explode_select)
pivot_operation = explode_select.groupBy('operation').pivot('key').sum('value')
display(pivot_operation)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select only columns needed for our Audit Log Table 

# COMMAND ----------


