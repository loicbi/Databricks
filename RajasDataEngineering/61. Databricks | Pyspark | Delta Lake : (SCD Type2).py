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

# MAGIC %md
# MAGIC ### Create table that will contain target data for scd 2 

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO default.scd2Demo VALUES (111, 'Unit1', 200, 500, 800, 400 , 'Y', current_timestamp(), '9999-12-31');
# MAGIC INSERT INTO default.scd2Demo VALUES (222, 'Unit2', 900, Null, 700, 100 , 'Y', current_timestamp(), '9999-12-31');
# MAGIC INSERT INTO default.scd2Demo VALUES (333, 'Unit3', 300, 900, 250, 650 , 'Y', current_timestamp(), '9999-12-31');

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- SELECT * FROM default.scd2demo;
# MAGIC -- or
# MAGIC SELECT * FROM delta.`/FileStore/tables/scd2Demo`;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 00: Create Target dataframe - Delta Status After Merge

# COMMAND ----------

# we use instance with method forPath/forName  ===> 55. Databricks| Pyspark| Delta Lake: Delta Table Instance
from delta.tables import *

targetDF = DeltaTable.forPath(spark, '/FileStore/tables/scd2Demo').toDF()

targetDF.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 01: Create Source dataframe with data to merge into table 'default.scd2Demo'

# COMMAND ----------

from pyspark.sql.types import * 
from pyspark.sql.functions import * 

schema = StructType([
    StructField('pk1', StringType(), True),\
    StructField('pk2', StringType(), True),\
    StructField('dim1', IntegerType(), True),\
    StructField('dim2', IntegerType(), True),\
    StructField('dim3', IntegerType(), True),\
    StructField('dim4', IntegerType(), True)\
])

# COMMAND ----------

data = [
    (111, 'Unit1', 200, 500, 800, 400),
    (222, 'Unit2', 800, 1300, 800, 500),
    (444, 'Unit4', 100, None, 700, 300),
]
sourceDF = spark.createDataFrame(data=data, schema = schema)
sourceDF.display()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: Left Join source to target

# COMMAND ----------

joinDF = sourceDF.join(targetDF, ((targetDF.pk1==sourceDF.pk1)  & (targetDF.pk2==sourceDF.pk2) & (targetDF.active_status==True)) , 'leftouter')
joinDF = joinDF.select(sourceDF['*'],\
    targetDF.pk1.alias('target_pk1'),\
    targetDF.pk2.alias('target_pk2'),\
    targetDF.dim1.alias('target_dim1'),\
    targetDF.dim2.alias('target_dim2'),\
    targetDF.dim3.alias('target_dim3'),\
    targetDF.dim4.alias('target_dim4'))
joinDF.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Filter out only changed records

# COMMAND ----------

# Calculates the hash code of given columns using the 64-bit variant of the xxHash algorithm, and returns the result as a long column. 

filterDF = joinDF.filter(xxhash64(joinDF.dim1,joinDF.dim2,joinDF.dim3,joinDF.dim4) != xxhash64(joinDF.target_dim1,joinDF.target_dim2,joinDF.target_dim3,joinDF.target_dim4) )

filterDF.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Create mergekey by combining key columns 

# COMMAND ----------

mergeDF = filterDF.withColumn('MERGEKEY', concat(filterDF.pk1, filterDF.pk2))
mergeDF.display()

# COMMAND ----------

# target_pk1 is not null ===> id already existed in target after union to mergeDF
dummyDF = filterDF.filter("target_pk1 is not null").withColumn('MERGEKEY', lit(None))
dummyDF.display()

# COMMAND ----------

scdDF = mergeDF.union(dummyDF)
scdDF.display()

# COMMAND ----------

from delta.tables import *

df_instance_target = DeltaTable.forPath(spark, '/FileStore/tables/scd2Demo')#.toDF()
        
df_instance_target.alias("target").merge(
    source = scdDF.alias('source'),
    condition = "source.MERGEKEY = concat(target.pk1,target.pk2) and target.active_status = 'Y'"
  ) \
  .whenMatchedUpdate(set =
    {
      "active_status": "'N'",
      "end_date": "current_date()"
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "pk1": "source.pk1",
      "pk2": "source.pk2",
      "dim1": "source.dim1",
      "dim2": "source.dim2",
      "dim3": "source.dim3",
      "dim4": "source.dim4",
      "active_status": "'Y'",
      "start_date": "current_date()",
      "end_date": "to_date('9999-12-31', 'yyyy-MM-dd')",
    }
  ).execute()

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- SHOW DATA 
# MAGIC SELECT * FROM delta.`/FileStore/tables/scd2Demo`;

# COMMAND ----------


