# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS default.scd2Demo;

# COMMAND ----------

dbutils.fs.rm('/FileStore/tables/scd2Demo', True)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE default.scd2Demo (
# MAGIC     pk1 INT,
# MAGIC     pk2 string, 
# MAGIC     dim1 int,
# MAGIC     dim2 int,
# MAGIC     dim3 int,
# MAGIC     dim4 int,
# MAGIC     active_status BOOLEAN,
# MAGIC     start_date TIMESTAMP,
# MAGIC     end_date TIMESTAMP
# MAGIC )
# MAGIC USING DELTA 
# MAGIC LOCATION '/FileStore/tables/scd2Demo';

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO default.scd2Demo VALUES (111, 'Unit1', 200, 500, 800, 400 , 'Y', current_timestamp(), '9999-12-31');
# MAGIC INSERT INTO default.scd2Demo VALUES (222, 'Unit2', 900, Null, 700, 100 , 'Y', current_timestamp(), '9999-12-31');
# MAGIC INSERT INTO default.scd2Demo VALUES (333, 'Unit3', 300, 900, 250, 650 , 'Y', current_timestamp(), '9999-12-31');

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO default.scd2Demo VALUES (666, 'Unit1', 200, 500, 800, 400 , 'Y', current_timestamp(), '9999-12-31');
# MAGIC INSERT INTO default.scd2Demo VALUES (777, 'Unit2', 900, Null, 700, 100 , 'Y', current_timestamp(), '9999-12-31');
# MAGIC INSERT INTO default.scd2Demo VALUES (888, 'Unit3', 300, 900, 250, 650 , 'Y', current_timestamp(), '9999-12-31');

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM  delta.`/FileStore/tables/scd2Demo`;

# COMMAND ----------

# MAGIC %sql 
# MAGIC DELETE FROM default.scd2demo WHERE pk1 = '666';

# COMMAND ----------

# MAGIC %sql 
# MAGIC UPDATE default.scd2demo SET dim1 = '2121' where pk1 = 777; 

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE HISTORY default.scd2Demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE default.scd2demo;

# COMMAND ----------


