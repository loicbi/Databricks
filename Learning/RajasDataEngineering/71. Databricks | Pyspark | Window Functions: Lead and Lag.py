# Databricks notebook source
# MAGIC %md
# MAGIC ### Create Data Frame 

# COMMAND ----------

simpledata = (
  ('James', 'Sales', 1000),\
  ('Michael', 'Sales', 2000),\
  ('Robert', 'Sales', 3000),\
  ('James', 'Sales', 4000),\
  ('Saif', 'Sales', 5000),\
  ('Maria', 'Finance', 6000),\
  ('Scott', 'Finance', 7000),\
  ('Jen', 'Finance', 8000),\
  ('Jeff', 'Marketing', 9000),\
  ('Kumar', 'Marketing', 10000),\
  )

columns = ['employee_name', 'department', 'salary']
df = spark.createDataFrame(data=simpledata, schema=columns)
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Wibndows Definition 

# COMMAND ----------

from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

windowSpec = Window.partitionBy('department').orderBy('salary')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lag Window Function 

# COMMAND ----------

from pyspark.sql.functions import lag
df.withColumn('lag', lag('salary', 1).over(windowSpec)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lead Window Function 

# COMMAND ----------

from pyspark.sql.functions import lead
df.withColumn('lead', lead('salary', 1).over(windowSpec)).show()
