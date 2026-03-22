# Databricks notebook source
# MAGIC %md
# MAGIC ## Create data Frame 

# COMMAND ----------


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# Create a list of data rows
data = [
    ( "Michael", "Physics", 80, "Pass", 95),
    ( "Michael", "Chemistry", 67, "Pass", 90),
    ( "Michael", "Mathematics", 78, "Pass", 90),

    ( "Nancy", "Physics", 30, "Pass", 80),
    ( "Nancy", "Chemistry", 59, "Pass", 80),
    ( "Nancy", "Mathematics", 75, "Pass", 80),

    ( "David", "Physics", 90, "Pass", 70),
    ( "David", "Chemistry", 87, "Pass", 70),
    ( "David", "Mathematics", 97, "Pass", 70),
    
    ( "John", "Physics", 33, "Pass", 60),
    ( "John", "Chemistry", 28, "Pass", 60),
    ( "John", "Mathematics", 52, "Pass", 60),
    
    ( "Blessy", "Physics", 89, "Pass", 75),
    ( "Blessy", "Chemistry", 76, "Pass", 75),
    ( "Blessy", "Mathematics", 63, "Pass", 75),
]

# Define the schema for the dataframe
schema = StructType([
    StructField("name", StringType(), True),
    StructField("subject", StringType(), True),
    StructField("mark", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("attendance", IntegerType(), True)
])

df = spark.createDataFrame(data=data, schema=schema).orderBy('name', 'subject')
df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Create ranking within each group of name 

# COMMAND ----------

from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window 

windowDept = Window.partitionBy('name').orderBy(col('mark').desc())
df2 = df.withColumn('row', row_number().over(windowDept)).orderBy('name', 'row')

display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get Top N rows per group Name 

# COMMAND ----------

df3 = df2.filter(col('row') <= 1)
display(df3)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create rank within group of subject 

# COMMAND ----------

from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window 

windowDept = Window.partitionBy('subject').orderBy(col('mark').desc())
df4 = df.withColumn('row', row_number().over(windowDept)).orderBy('name', 'row')
display(df4)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get Top N rows per group Subject 

# COMMAND ----------

df5 = df4.filter(col('row') <= 1)
display(df5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reverse Rank to get Bottom N rows Per Group

# COMMAND ----------

windowDept = Window.partitionBy('subject').orderBy(col('mark'))
df6 = df.withColumn('row', row_number().over(windowDept)).orderBy('name', 'row')
display(df6)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get Bottom N Rows Per Group 

# COMMAND ----------

df7 = df6.filter(col('row') <= 1)
df7.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Other Example
# MAGIC https://sparkbyexamples.com/pyspark/pyspark-retrieve-top-n-from-each-group-of-dataframe/

# COMMAND ----------

from pyspark.sql import SparkSession,Row
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

data = [("James","Sales",3000),("Michael","Sales",4600),
      ("Robert","Sales",4100),("Maria","Finance",3000),
      ("Raman","Finance",3000),("Scott","Finance",3300),
      ("Jen","Finance",3900),("Jeff","Marketing",3000),
      ("Kumar","Marketing",2000)]

df = spark.createDataFrame(data,["Name","Department","Salary"])
df.show()

from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
windowDept = Window.partitionBy("department").orderBy(col("salary").desc())
df.withColumn("row",row_number().over(windowDept))\
  .filter(col("row") <= 2)\
  .drop("row").show()

df.createOrReplaceTempView("EMP")
spark.sql("select Name, Department, Salary from "+
     " (select *, row_number() OVER (PARTITION BY department ORDER BY salary DESC) as rn " +
     " FROM EMP) tmp where rn <= 2").show()

# COMMAND ----------


