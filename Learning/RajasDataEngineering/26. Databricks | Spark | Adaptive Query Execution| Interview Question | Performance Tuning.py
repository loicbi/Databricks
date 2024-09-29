# Databricks notebook source
# MAGIC %md
# MAGIC  l'optimiseur AQE d'Apache Spark est un moteur d'optimisation avancé qui ajuste dynamiquement le plan d'exécution pour une requête donnée, en fonction des statistiques d'exécution des données. Il offre de nombreux avantages, notamment l'optimisation dynamique, la planification adaptative, l'optimisation basée sur les coûts, la réduction des frais généraux et un traitement plus rapide. L'optimiseur AQE est une fonctionnalité essentielle d'Apache Spark et une avancée significative dans la technologie de traitement du Big Data.

# COMMAND ----------

from pyspark.sql.functions import  sum 
from pyspark.sql.types import IntegerType

# COMMAND ----------


# Enable AQE
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.optimizerEnabled", "true")

# Lire les données à partir d'un CSV
df = spark.read.csv( "dbfs:/FileStore/tables/stream_read/amazon_prime_users_2.csv" , header= True , inferSchema= True ) 
# # Regrouper par colonne 'Membership Start Date' et somme de la colonne 'Feedback/Ratings'
df = df.groupBy("Membership Start Date").agg(sum("Feedback/Ratings").alias("Feedback/Ratings")).orderBy('Membership Start Date')


# Filtrer les lignes où 'B' est supérieur à 100
df = df.filter(df['Membership Start Date'] > '2024-01-01' )

# Mettre en cache le DataFrame résultant
df.cache()

# Compter le nombre de lignes dans le DataFrame 
print(df.count())

# Annuler la persistance du DataFrame
df.unpersist()
display(df)

# COMMAND ----------


