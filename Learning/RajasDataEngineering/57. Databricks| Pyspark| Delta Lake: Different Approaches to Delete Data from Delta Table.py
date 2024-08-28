# Databricks notebook source
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
# MAGIC ### Populate Sample Data

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO default.employee_demo VALUES(100, 'Stephen', 'M', 2000, 'IT');
# MAGIC INSERT INTO default.employee_demo VALUES(200, 'Fred', 'M', 2500, 'HR');
# MAGIC INSERT INTO default.employee_demo VALUES(300, 'Assogba', 'M', 8000, 'SALES');
# MAGIC INSERT INTO default.employee_demo VALUES(400, 'Beatrice', 'M', 2000, 'IT');
# MAGIC INSERT INTO default.employee_demo VALUES(500, 'Andre', 'M', 2500, 'HR');
# MAGIC INSERT INTO default.employee_demo VALUES(600, 'SEKA', 'M', 2500, 'HR');
# MAGIC INSERT INTO default.employee_demo VALUES(700, 'Apo', 'M', 2500, 'HR');

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 1: SQL

# COMMAND ----------

# MAGIC %sql 
# MAGIC DELETE FROM default.employee_demo WHERE employee_id = '700'; 
# MAGIC SELECT * FROM default.employee_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 1: SQL unsing delta location

# COMMAND ----------

# MAGIC %sql 
# MAGIC DELETE FROM delta.`/FileStore/tables/delta/path_emplyee_demo` where employee_id = '100'; 
# MAGIC SELECT * FROM DELTA.`/FileStore/tables/delta/path_emplyee_demo`;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 1: Pyspark Delta Table Instance

# COMMAND ----------

from delta.tables import * 
from pyspark.sql.functions import * 

df_instance = DeltaTable.forPath(spark, '/FileStore/tables/delta/path_emplyee_demo')


# Declare the predicate by using a SQL-formatted string.
df_instance.delete("employee_id >= '600'")

# Declare the predicate by using Spark SQL functions.
df_instance.delete(col('employee_id') == '400')

display(df_instance.toDF())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Multiple conditions using SQL Predicate 

# COMMAND ----------

df_instance.delete("employee_id = '300' and Dept = 'SALES'")
display(df_instance.toDF())

# COMMAND ----------


