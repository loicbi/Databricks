# Databricks notebook source
# MAGIC %md
# MAGIC ### Prepare Data

# COMMAND ----------

# Prepare Data
simpleData = (("Java",4000,5), \
    ("Python", 4600,10),  \
    ("Scala", 4100,15),   \
    ("Scala", 4500,15),   \
    ("PHP", 3000,20),  \
  )
columns= ["CourseName", "fee", "discount"]

# Create DataFrame
df = spark.createDataFrame(data = simpleData, schema = columns)
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Greatest of columns 

# COMMAND ----------

from pyspark.sql.functions import greatest

greatDf = df.withColumn('Greatest', greatest("fee", "discount"))

greatDf.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Least of columns 

# COMMAND ----------

from pyspark.sql.functions import least

leastDf = df.withColumn('Least', least("fee", "discount"))

leastDf.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Max of columns 

# COMMAND ----------

from pyspark.sql.functions import max, col

maxColumnDf = df.select(max(col('discount')).alias('Max_Discount'),\
    max(col('fee')).alias('Max_Fee'))

maxColumnDf.show(truncate=False)

# or Agg Max

df.agg({'discount':'max','fee':'max'}).show(truncate=False)



# PySpark SQL MAX
df.createOrReplaceTempView("COURSE")
spark.sql("SELECT MAX(FEE) FROM COURSE").show()
spark.sql("SELECT MAX(FEE), MAX(DISCOUNT) FROM COURSE").show()
spark.sql("SELECT COURSENAME,MAX(FEE) FROM COURSE GROUP BY COURSENAME").show()

# COMMAND ----------


