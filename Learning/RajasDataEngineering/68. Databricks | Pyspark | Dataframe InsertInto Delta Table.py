# Databricks notebook source
data = [("ABC pvt ltd", "Q1", 2000),
        ("XYZ pvt ltd", "Q1", 5000),
        ("KLM pvt ltd", "Q1", 2000)]

column = ["Company", "Quarter", "Revenue"]

df = spark.createDataFrame(data=data, schema=column)

# COMMAND ----------

spark.sql(" DROP TABLE IF EXISTS default.fact_revenue ")

df.write.saveAsTable('default.fact_revenue')

# COMMAND ----------

display(spark.table('default.fact_revenue'))

# COMMAND ----------

data = [("RST pvt ltd", "Q4", 7000)]
column = ["Company", "Quarter", "Revenue"]


df1 = spark.createDataFrame(data=data, schema=column )

df1.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Use insertInto    ---  overwrite=False

# COMMAND ----------

df1.write.insertInto('default.fact_revenue', overwrite=False)

display(spark.table('default.fact_revenue'))

# COMMAND ----------

data = [("QRT pvt ltd", "Q3", 3000)]
column = ["Company", "Quarter", "Revenue"]
df2 = spark.createDataFrame(data=data, schema=column )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use insertInto    ---  overwrite=True

# COMMAND ----------

df2.write.insertInto('default.fact_revenue', overwrite=True)

display(spark.table('default.fact_revenue'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use SQL    ---  using tempView

# COMMAND ----------

data = [("QRT pvt ltd", "Q4", 9855)]
column = ["Company", "Quarter", "Revenue"]
df3 = spark.createDataFrame(data=data, schema=column)


df3.createOrReplaceTempView('v_insert_data_df3')

spark.sql("""
          INSERT INTO default.fact_revenue 
           SELECT * FROM v_insert_data_df3;
          """)

display(spark.table('default.fact_revenue'))

# COMMAND ----------


