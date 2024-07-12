# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS default.scd2Demo;

# COMMAND ----------

dbutils.fs.rm('/FileStore/tables/scd2Demo', True)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS default.scd2Demo (
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

# MAGIC %sql
# MAGIC INSERT INTO default.scd2Demo VALUES (111, 'Unit1', 200, 500, 800, 400 , 'Y', current_timestamp(), '9999-12-31');
# MAGIC INSERT INTO default.scd2Demo VALUES (222, 'Unit2', 900, Null, 700, 100 , 'Y', current_timestamp(), '9999-12-31');
# MAGIC INSERT INTO default.scd2Demo VALUES (333, 'Unit3', 300, 900, 250, 650 , 'Y', current_timestamp(), '9999-12-31');

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO default.scd2Demo VALUES (666, 'Unit1', 200, 500, 800, 400 , 'Y', current_timestamp(), '9999-12-31');
# MAGIC INSERT INTO default.scd2Demo VALUES (777, 'Unit2', 900, Null, 700, 100 , 'Y', current_timestamp(), '9999-12-31');
# MAGIC INSERT INTO default.scd2Demo VALUES (888, 'Unit3', 300, 900, 250, 650 , 'Y', current_timestamp(), '9999-12-31');

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM delta.`/FileStore/tables/scd2Demo`;

# COMMAND ----------

# MAGIC %sql 
# MAGIC DELETE FROM default.scd2demo WHERE pk1 = '666';

# COMMAND ----------

# MAGIC %sql 
# MAGIC UPDATE default.scd2demo SET dim1=100 WHERE pk1=777;

# COMMAND ----------

# MAGIC %md
# MAGIC La commande "OPTIMIZE" dans Delta Lake est utilisée pour optimiser la table en fusionnant les petits fichiers en plus gros fichiers

# COMMAND ----------

# MAGIC %sql 
# MAGIC optimize default.scd2demo

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql 
# MAGIC describe history default.scd2Demo;

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM default.scd2demo;

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/scd2Demo

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL APPROACH 
# MAGIC En exécutant la commande "VACUUM" sur une table Delta Lake, vous pouvez supprimer les fichiers obsolètes qui ne sont plus nécessaires pour maintenir la cohérence des données. La commande "VACUUM" supprime les fichiers qui ont été marqués comme obsolètes et qui sont plus anciens que la valeur spécifiée pour la durée de rétention. Cela permet de libérer de l'espace sur le système de fichiers et d'optimiser les performances de lecture des données.
# MAGIC
# MAGIC
# MAGIC
# MAGIC La différence entre les deux commandes est que la première, "VACUUM default.scd2Demo RETAIN 1 HOURS dry run", effectue une simulation de l'exécution de la commande "VACUUM", tandis que la deuxième, "VACUUM default.scd2Demo RETAIN 1 HOURS", exécute réellement la commande.
# MAGIC
# MAGIC Lorsque vous utilisez l'option "dry run", Delta Lake simule l'exécution de la commande "VACUUM" sans effectuer réellement la suppression des fichiers. Cela permet de voir quels fichiers seraient supprimés si la commande était exécutée, sans risque de perte de données. La simulation peut être utile pour vérifier les fichiers qui seraient supprimés avant de les supprimer réellement.
# MAGIC
# MAGIC En revanche, lorsque vous exécutez la commande sans l'option "dry run", Delta Lake effectue réellement la suppression des fichiers obsolètes qui sont plus anciens que la durée de rétention spécifiée. Cela permet de libérer de l'espace de stockage en supprimant les fichiers qui ne sont plus nécessaires.
# MAGIC
# MAGIC Il est important de noter que la durée de rétention spécifiée dans la commande "VACUUM" détermine combien de temps les fichiers doivent être conservés avant d'être supprimés. Les fichiers qui ont été marqués comme obsolètes et qui sont plus anciens que cette durée de rétention seront supprimés lors de l'exécution de la commande.

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- par default c'est 7 jours ===> 168 hours 
# MAGIC VACUUM default.scd2Demo RETAIN 1 HOURS DRY RUN ;

# COMMAND ----------

# MAGIC %md
# MAGIC L'exemple de commande que vous avez donné, "VACUUM default.scd2Demo RETAIN 720 HOURS DRY RUN", simulerait l'exécution de la commande "VACUUM" sur la table "default.scd2Demo" avec une durée de rétention de 720 heures (30 jours). Cela signifie que seuls les fichiers qui ont été marqués comme obsolètes et qui sont plus anciens que 720 heures seraient supprimés.

# COMMAND ----------

# MAGIC %sql 
# MAGIC VACUUM default.scd2Demo RETAIN 720 HOURS DRY RUN; 

# COMMAND ----------

# MAGIC %md
# MAGIC En désactivant cette vérification en utilisant la commande "SET spark.databricks.delta.retentionDurationCheck.enabled = False", Delta Lake n'effectuera plus cette vérification et vous pourrez supprimer les fichiers même s'ils sont encore dans la période de rétention spécifiée. Cela peut être utile dans certains cas où vous souhaitez supprimer les fichiers immédiatement, indépendamment de la durée de rétention.
# MAGIC
# MAGIC Il est important de noter que la désactivation de la vérification de la durée de rétention peut entraîner la suppression de fichiers qui pourraient encore être utilisés par d'autres processus ou requêtes. Par conséquent, assurez-vous de comprendre les implications avant de désactiver cette vérification.

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- maintenant si nous voulons evacuer les fichiers indesirables dans le delta en modiant le default 168 hours , on utilise ce script 
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = False;

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- check list files will delete that existed before 1 hour 
# MAGIC
# MAGIC VACUUM default.scd2Demo RETAIN 1 HOURS dry run ; 

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/scd2Demo

# COMMAND ----------

# MAGIC %md
# MAGIC ### PYTHON APPROACH 
# MAGIC https://docs.delta.io/latest/delta-utility.html#-delta-vacuum&language-python

# COMMAND ----------

from delta.tables import *
df_instance_vacuum = DeltaTable.forPath(spark, '/FileStore/tables/scd2Demo')

df_instance_vacuum.vacuum()        # vacuum files not required by versions older than the default retention period

# df_instance_vacuum.vacuum(100)     # vacuum files not required by versions more than 100 hours old
