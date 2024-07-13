# Databricks notebook source
# MAGIC %md
# MAGIC **What is UDF?**
# MAGIC
# MAGIC PySpark UDF is a User Defined Function that is used to create a reusable function in Spark.
# MAGIC
# MAGIC Once UDF created, that can be re-used on multiple DataFrames and SQL (after registering).
# MAGIC
# MAGIC The default type of the udf() is StringType.
# MAGIC
# MAGIC You need to handle nulls explicitly otherwise you will see side-effects.
# MAGIC
# MAGIC The PySpark UDF (User Define Function) that is used to define a new Column-based function.
# MAGIC
# MAGIC --
# MAGIC
# MAGIC PySpark UDF est une fonction définie par l'utilisateur utilisée pour créer une fonction réutilisable dans Spark.
# MAGIC
# MAGIC Une fois l'UDF créé, il peut être réutilisé sur plusieurs DataFrames et SQL (après enregistrement).
# MAGIC
# MAGIC Le type par défaut de udf() est StringType.
# MAGIC
# MAGIC Vous devez gérer les valeurs nulles explicitement, sinon vous verrez des effets secondaires.
# MAGIC
# MAGIC L'UDF PySpark (User Define Function) utilisée pour définir une nouvelle fonction basée sur les colonnes.
# MAGIC

# COMMAND ----------

emp_data = [(1, "Alice", 25, "Marketing", 50000, "2023-01-01"),
       (2, "Bob", 30, "Sales", 60000, "2023-02-01"),
       (3, "Charlie", 40, "HR", 45000, "2023-03-01"),
      (13, "Michael", 27, "Marketing", 47000, "2023-04-01"),
       (4, "David", 35, "Finance", 55000, "2023-05-01"),
       (5, "Eve", 28, "Marketing", 48000, "2023-06-01"),
       (6, "Frank", 50, "Sales", 70000, "2023-07-01"),
       (7, "George", 45, "HR", 52000, "2023-08-01"),
       (8, "Hannah", 32, "Finance", 58000, "2023-09-01"),]

schema = ["id", "name", "age", "department", "salary", "date_joined"]

empDf = spark.createDataFrame(data=emp_data, schema=schema)

display(empDf)





# COMMAND ----------

# MAGIC %md
# MAGIC ## Define UDF to Rename Columns 

# COMMAND ----------

import pyspark.sql.functions as f

def rename_columns(rename_df):
    for column in rename_df.columns:
        rename_df = rename_df.withColumnRenamed(column, f'UDF____{column}')
    return rename_df



# COMMAND ----------

# MAGIC %md
# MAGIC ### Execute UDF 

# COMMAND ----------

rename_df = rename_columns(empDf)
display(rename_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### UDF to convert name into UPPER CASE

# COMMAND ----------

from pyspark.sql.functions import upper, col
from pyspark.sql.types import *

@udf(returnType=StringType())
def upperValue(value):
    return value.upper()

# COMMAND ----------

df = empDf.withColumn('Upper___Name', upperValue(col('name')))
display(df)

# COMMAND ----------


