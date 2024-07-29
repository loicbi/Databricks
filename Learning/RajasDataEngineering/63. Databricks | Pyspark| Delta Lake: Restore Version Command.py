# Databricks notebook source
# MAGIC %md
# MAGIC ### CREATE DELTA TABLE 

# COMMAND ----------

from delta.tables import * 

DeltaTable.createIfNotExists(spark)\
  .tableName('scd2Demo')\
    .addColumn('pk1', 'INT')\
      .addColumn('pk2', 'STRING')\
        .addColumn('dim1', 'INT')\
          .addColumn('dim2', 'INT')\
           .addColumn('dim3', 'INT')\
            .addColumn('dim4', 'INT')\
              .addColumn('active_status', 'BOOLEAN')\
                .addColumn('start_date', 'TIMESTAMP')\
                  .addColumn('end_date', 'TIMESTAMP')\
                    .location('/FileStore/tables/scd2Demo')\
                      .execute()
              

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- insert data 
# MAGIC SELECT * FROM DELTA.`/FileStore/tables/scd2Demo`;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create table instance 

# COMMAND ----------

df_instance_target_table = DeltaTable.forPath(spark, '/FileStore/tables/scd2Demo')
df_instance_target_table.toDF().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### List down version 

# COMMAND ----------

df_instance_target_table.history().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Restore using version number 

# COMMAND ----------

df_instance_target_table.restoreToVersion(3)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from default.scd2demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Restore using version timestamp 

# COMMAND ----------

df_instance_target_table.restoreToTimestamp('2024-06-14T19:56:18Z')

# COMMAND ----------

df_instance_target_table.toDF().display()

# COMMAND ----------

df_instance_target_table.history().display()
