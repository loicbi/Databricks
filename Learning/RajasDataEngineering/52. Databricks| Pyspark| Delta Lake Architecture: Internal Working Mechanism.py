# Databricks notebook source
# MAGIC %md
# MAGIC ### Delete table and his path dbfs  

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS delta_internal_table;

# COMMAND ----------

dbutils.fs.rm("/FileStore/tables/delta", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Delta Lake 

# COMMAND ----------

from delta.tables import *

DeltaTable.createOrReplace(spark)\
    .tableName('default.delta_internal_table')\
    .addColumn('emp_id', 'int')\
    .addColumn('emp_name', 'string')\
    .addColumn('gender', 'string')\
    .addColumn('salary', 'int')\
    .addColumn('Dept', 'string')\
    .property('description', 'table  created for demo purpose')\
    .location('/FileStore/tables/delta/arch__demo').execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY default.delta_internal_table;

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/delta/arch__demo/_delta_log

# COMMAND ----------

# MAGIC %fs
# MAGIC head /FileStore/tables/delta/arch__demo/_delta_log/00000000000000000000.json

# COMMAND ----------

display(spark.read.format('delta').load('/FileStore/tables/delta/arch__demo/'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM default.delta_internal_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO default.delta_internal_table VALUES(100, 'Stephen', 'M', 2000, 'IT');

# COMMAND ----------

# MAGIC %fs
# MAGIC head /FileStore/tables/delta/arch__demo/_delta_log/00000000000000000001.json

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO default.delta_internal_table VALUES(200, 'Messi', 'M', 2500, 'HR');
# MAGIC INSERT INTO default.delta_internal_table VALUES(300, 'Ronaldinho', 'M', 8000, 'SALES');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM default.delta_internal_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO default.delta_internal_table VALUES(100, 'Stephen', 'M', 2000, 'IT');
# MAGIC INSERT INTO default.delta_internal_table VALUES(200, 'Fred', 'M', 2500, 'HR');
# MAGIC INSERT INTO default.delta_internal_table VALUES(300, 'Assogba', 'M', 8000, 'SALES');
# MAGIC
# MAGIC
# MAGIC INSERT INTO default.delta_internal_table VALUES(100, 'Stephen', 'M', 2000, 'IT');
# MAGIC INSERT INTO default.delta_internal_table VALUES(200, 'Fred', 'M', 2500, 'HR');
# MAGIC INSERT INTO default.delta_internal_table VALUES(300, 'Assogba', 'M', 8000, 'SALES');

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT COUNT(*) FROM default.delta_internal_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM default.delta_internal_table WHERE emp_id=100;
# MAGIC -- DELETE FROM default.delta_internal_table WHERE emp_id=200;
# MAGIC -- DELETE FROM default.delta_internal_table WHERE emp_id=300;

# COMMAND ----------

# MAGIC %fs
# MAGIC head /FileStore/tables/delta/arch__demo/_delta_log/00000000000000000010.json

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/delta/arch__demo/_delta_log

# COMMAND ----------

# get 
display(spark.read.format('parquet').load('/FileStore/tables/delta/arch__demo/_delta_log/00000000000000000010.checkpoint.parquet'))

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP TABLE IF EXISTS default.delta_internal_table;

# COMMAND ----------


