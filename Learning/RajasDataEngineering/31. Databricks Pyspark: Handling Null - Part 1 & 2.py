# Databricks notebook source
# MAGIC %md
# MAGIC # PART 1

# COMMAND ----------

# Create DataFrames from sample data
# Create a list of dictionaries containing employee data
employees = [
    {'Name': 'John Doe', 'Age': 30, 'Department': 'Marketing'},
    {'Name': 'Jane', 'Age': 25, 'Department': 'Sales'},
    {'Name': 'Bob Johnson', 'Age': 40, 'Department': None},
    {'Name': 'Loic', 'Age': 40, 'Department': 'Engineering'},
    {'Name': 'Assogba', 'Age': None, 'Department': None},
    {'Name': 'AWS', 'Age': None, 'Department': 'Engineering'},
]

df = spark.createDataFrame(data=employees)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter all records with null 

# COMMAND ----------

# df_with_null = df.filter(df.Department.isNull())
# display(df_with_null)

# OR 
from pyspark.sql.functions import *
display(df.filter((col('Department').isNotNull()) & (col('Age').isNull())))

# COMMAND ----------

# MAGIC %md
# MAGIC #PART 2

# COMMAND ----------

# Create the DataFrame
schema = ["id", "name", "age", "department", "salary"]
data = [(1, "Alice", 25, "Marketing", None),
       (2, "Bob", 30, "Sales", 60000),
       (3, "Charlie", 40, "HR", 45000),
      (13, "Michael", 27, "Marketing", 47000),
       (4, "David", 35, "Finance", None),
       (5, "Eve", 28, None, None),
       (6, "Frank", 50, "Sales", 70000),
       (7, "George", 45, "HR", 52000),
       (8, "Hannah", 32, "Finance", None),
       (9, "Ian", 24, None, 46000),
       (10, "Jack", 38, "Sales", 62000),
       (None,None,None,None,None),]

employee_df = spark.createDataFrame(data=data, schema=schema)
display(employee_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### na.drop() 
# MAGIC Drops the rows if any or all columns contain Null value

# COMMAND ----------

display(employee_df.dropna()) # # or  display(employee_df.na.drop())

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### Drops the records with Null Value - ALL & ANY

# COMMAND ----------

# display(employee_df.na.drop('all')) # all => all columns contain null 
display(employee_df.na.drop('any')) # any => if column contains  null 

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### Drops the records with Null Value - on selected column

# COMMAND ----------

display(employee_df.na.drop(subset=['department', 'name']))

# COMMAND ----------

# MAGIC %md
# MAGIC #### na.fill() 
# MAGIC Populates dummy value for all Null values given as parameter to this function
# MAGIC
# MAGIC Remplit la valeur factice pour toutes les valeurs Null données en paramètre à cette fonction

# COMMAND ----------

# MAGIC %md
# MAGIC ### FILL VALUE ALL COLUMNS IF NULL IS PRESENT 

# COMMAND ----------

display(employee_df)
# display(employee_df.na.fill(value=0)) # for columns integer 
display(employee_df.na.fill(value='NA')) # for columns string 

# COMMAND ----------


