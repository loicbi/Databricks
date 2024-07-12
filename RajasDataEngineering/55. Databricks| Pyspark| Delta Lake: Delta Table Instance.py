# Databricks notebook source
# MAGIC %md
# MAGIC ### Approach 1: Using forPath

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS default.employee_demo;

# COMMAND ----------

dbutils.fs.rm('/FileStore/tables/delta/create_table', True)

# COMMAND ----------

from delta.tables import *

DeltaTable.createOrReplace(spark)\
    .tableName('default.employee_demo')\
    .addColumn('emp_id', 'int')\
    .addColumn('emp_name', 'string')\
    .addColumn('gender', 'string')\
    .addColumn('salary', 'int')\
    .addColumn('Dept', 'string')\
    .property('description', 'table  created for demo purpose')\
    .location('/FileStore/tables/delta/create_table').execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO default.employee_demo VALUES(100, 'Stephen', 'M', 2000, 'IT');
# MAGIC INSERT INTO default.employee_demo VALUES(200, 'Fred', 'M', 2500, 'HR');
# MAGIC INSERT INTO default.employee_demo VALUES(200, 'Fred', 'M', 2500, 'HR');
# MAGIC INSERT INTO default.employee_demo VALUES(300, 'Assogba', 'M', 8000, 'SALES');
# MAGIC
# MAGIC
# MAGIC INSERT INTO default.employee_demo VALUES(100, 'Stephen', 'M', 2000, 'IT');
# MAGIC INSERT INTO default.employee_demo VALUES(200, 'Fred', 'M', 2500, 'HR');
# MAGIC INSERT INTO default.employee_demo VALUES(200, 'Fred', 'M', 2500, 'HR');
# MAGIC INSERT INTO default.employee_demo VALUES(200, 'Fred', 'M', 2500, 'HR');
# MAGIC INSERT INTO default.employee_demo VALUES(200, 'Fred', 'M', 2500, 'HR');
# MAGIC INSERT INTO default.employee_demo VALUES(300, 'Assogba', 'M', 8000, 'SALES');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM default.employee_demo;

# COMMAND ----------

'''Dans le contexte d'un Delta Lake, une instance fait référence à une exécution d'un cluster ou d'une machine virtuelle qui traite et manipule les données stockées dans un Delta Lake.
'''

deltaInstance_1 = DeltaTable.forPath(spark, '/FileStore/tables/delta/create_table')

deltaInstance_1.toDF().display()

# delete 
deltaInstance_1.delete(' emp_id = \'200\'')

deltaInstance_1.toDF().display()

# COMMAND ----------

# MAGIC %sql
# MAGIC --  delete
# MAGIC -- DELETE FROM default.delta_internal_table WHERE emp_id = '100';
# MAGIC -- or
# MAGIC DELETE FROM delta.`/FileStore/tables/delta/create_table` WHERE emp_id = '100';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Approach 2: Using forName

# COMMAND ----------

deltaInstance_2 = DeltaTable.forName(spark, tableOrViewName = 'default.employee_demo')
deltaInstance_2.toDF().display()

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY default.employee_demo;

# COMMAND ----------

display(deltaInstance_2.history())

# COMMAND ----------


