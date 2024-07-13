# Databricks notebook source
# MAGIC %md
# MAGIC ### CREATE DELTA LAKE TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS default.employee_demo;

# COMMAND ----------

dbutils.fs.rm('/FileStore/tables/delta/path_emplyee_demo', True)

# COMMAND ----------

from delta.tables import *

DeltaTable.createOrReplace(spark)\
    .tableName('default.employee_demo')\
    .addColumn('employee_id', 'int')\
    .addColumn('emp_name', 'string')\
    .addColumn('gender', 'string')\
    .addColumn('salary', 'int')\
    .addColumn('Dept', 'string')\
    .property('description', 'table  created for demo purpose')\
    .location('/FileStore/tables/delta/path_emplyee_demo').execute()

display(spark.sql('SELECT * FROM default.employee_demo;'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL Insert 

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO default.employee_demo VALUES(100, 'Stephen', 'M', 2000, 'IT');

# COMMAND ----------

display(spark.sql('SELECT * FROM default.employee_demo;'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### DataFrame Insert 

# COMMAND ----------

#  Create Data Frame 

from pyspark.sql.types import * 
# create df 
employee_data = [
    (100, "Stephen", "M", 2000, "IT"),
    (200, "Philipp", "M", 2000, "HR"),
    (300, "Lara", "F", 6000, "SALES"),
]

employee_schema = StructType([
    StructField('employee_id', IntegerType(), False),
    StructField('emp_name', StringType(), True),
    StructField('gender', StringType(), True),
    StructField('salaray', IntegerType(), True),
    StructField('dept', StringType(), True),
])

df = spark.createDataFrame(data=employee_data, schema=employee_schema)
df.show(truncate=False)

# COMMAND ----------


# spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", True)

# Insert DataFrame 
df.write.format('delta').mode('append').saveAsTable('default.employee_demo')

display(spark.sql('SELECT * FROM default.employee_demo;'))

# COMMAND ----------

from delta.tables import * 
# describe 
df_instance = DeltaTable.forName(spark, 'default.employee_demo')

df_instance.history().display()

df_instance.toDF().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DataFrame InsertInto() method 

# COMMAND ----------


# create df 
employee_data = [
    (100, "Stephen", "M", 2000, "IT"),
]

employee_schema = StructType([
    StructField('employee_id', IntegerType(), False),
    StructField('emp_name', StringType(), True),
    StructField('gender', StringType(), True),
    StructField('salaray', IntegerType(), True),
    StructField('dept', StringType(), True),
])

df = spark.createDataFrame(data=employee_data, schema=employee_schema)
df.show(truncate=False)

# method 
df.write.insertInto('default.employee_demo', overwrite=False)

# show table 
# display(spark.sql('SELECT * FROM default.employee_demo;'))


# COMMAND ----------


