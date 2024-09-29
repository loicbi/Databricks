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

# MAGIC %sql
# MAGIC INSERT INTO default.employee_demo VALUES(100, 'Stephen', 'M', 2000, 'IT');
# MAGIC INSERT INTO default.employee_demo VALUES(200, 'Fred', 'M', 2500, 'HR');
# MAGIC INSERT INTO default.employee_demo VALUES(300, 'Assogba', 'M', 8000, 'SALES');
# MAGIC INSERT INTO default.employee_demo VALUES(400, 'Beatrice', 'M', 2000, 'IT');
# MAGIC INSERT INTO default.employee_demo VALUES(500, 'Andre', 'M', 2500, 'HR');
# MAGIC INSERT INTO default.employee_demo VALUES(600, 'SEKA', 'M', 2500, 'HR');
# MAGIC INSERT INTO default.employee_demo VALUES(700, 'Apo', 'M', 2500, 'HR');
# MAGIC
# MAGIC SELECT * FROM delta.`/FileStore/tables/delta/path_emplyee_demo`;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Methode 1: SQL Standard using Table Path 

# COMMAND ----------

# MAGIC %sql 
# MAGIC UPDATE delta.`/FileStore/tables/delta/path_emplyee_demo` SET emp_name = 'BEATRICE' WHERE employee_id = '100';
# MAGIC
# MAGIC SELECT * FROM delta.`/FileStore/tables/delta/path_emplyee_demo`;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 2: Pyspark Standard using table instance  

# COMMAND ----------

from pyspark.sql.functions import *
from delta.tables import * 

df_instance = DeltaTable.forPath(spark, '/FileStore/tables/delta/path_emplyee_demo')


# # Declare the predicate by using a SQL-formatted string.
df_instance.update(
    condition = "employee_id = '200'" ,
    set= {
        "salary" : "105000",
        "Dept": "'IT'"
    }
)


# Declare the predicate by using Spark SQL functions.

df_instance.update(
    condition =  col('employee_id').isin(['400', '100']),
    set = {
        "gender": lit('F'),
        "Dept": lit('TC')
    }
)
display(df_instance.toDF().filter(col('employee_id').isin(['200', '400', '100'])))

# COMMAND ----------


