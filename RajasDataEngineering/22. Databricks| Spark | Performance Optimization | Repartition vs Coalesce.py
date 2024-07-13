# Databricks notebook source
# MAGIC %md
# MAGIC #clés à retenir:
# MAGIC
# MAGIC Utilisez Spark Repartition() lorsque les données doivent être réparties uniformément sur les partitions pour une meilleure efficacité du traitement parallèle.
# MAGIC
# MAGIC Utilisez Spark Coalesce() lorsque le nombre de partitions doit être réduit pour améliorer les performances sans opérations de brassage complètes coûteuses.
# MAGIC
# MAGIC Lorsque vous utilisez Repartition() ou Coalesce(), tenez compte du mouvement des données, des performances et du nombre de partitions qui en résultent pour une meilleure optimisation des tâches Spark.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check default parameters 

# COMMAND ----------

sc.defaultParallelism

# COMMAND ----------

spark.conf.get("spark.sql.files.maxPartitionBytes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generating data within spark environment 

# COMMAND ----------

# rdd = sc.parallelize(range(1, 11))
# rdd = sc.getNumPartitions()
from pyspark.sql.types import * 

df = spark.createDataFrame(range(10), IntegerType())

df.rdd.getNumPartitions()



# COMMAND ----------

# Generating data within spark environment 
df.rdd.glom().collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read external file in spark 

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/stream_read/')

# COMMAND ----------

df = spark.read.format('csv').option('inferSchema', True).option('sep', ',').load('/FileStore/tables/stream_read/')
df.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Change the maxpartitionbytes parameter which changes in no of partitions 

# COMMAND ----------

# edit partition maxPartitionBytes 
spark.conf.set("spark.sql.files.maxPartitionBytes", 200000)

# show 
spark.conf.get("spark.sql.files.maxPartitionBytes")

# COMMAND ----------

df = spark.read.format('csv').option('inferSchema', True).option('sep', ',').option('header', True).load('/FileStore/tables/stream_read/')
df.rdd.getNumPartitions()

# COMMAND ----------

from pyspark.sql.functions import spark_partition_id

df2=df.repartition(5).withColumn("partitionId",spark_partition_id())
df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create single a single partition with all data. This is not good for performance, as one core would process entire data while all other cores are kept idle
# MAGIC ### Créez une seule partition avec toutes les données. Ce n'est pas bon pour les performances, car un cœur traiterait des données entières tandis que tous les autres cœurs resteraient inactifs.

# COMMAND ----------

# one parallelisme 
rdd2 = sc.parallelize(range(100), 1)
rdd2.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Repartition 

# COMMAND ----------

from pyspark.sql.types import IntegerType
df = spark.createDataFrame(range(10), IntegerType())
df.rdd.glom().collect()

# COMMAND ----------

df1 = df.repartition(5)
df1.rdd.getNumPartitions()

# COMMAND ----------

df1.rdd.glom().collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Coalesce 

# COMMAND ----------

df2 = df.coalesce(2)

df2.rdd.getNumPartitions()

df2.rdd.glom().collect()
