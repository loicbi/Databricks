# Databricks notebook source
# MAGIC %md
# MAGIC La commande Z-Order dans Delta Lake est utilisée pour optimiser le placement des données au sein d'une table Delta en fonction d'une ou plusieurs colonnes. Le Z-Ordering peut améliorer les performances des requêtes en regroupant les données fréquemment consultées ensemble sur le disque, ce qui réduit la quantité de données à lire lors de l'exécution des requêtes.
# MAGIC
# MAGIC Pour utiliser la commande Z-Order, vous devez spécifier les colonnes à utiliser pour l'ordonnancement des données dans la table Delta. Par exemple, si vous avez une table Delta appelée "maTable" et que vous souhaitez ordonner les données en fonction des colonnes "col1" et "col2", vous pouvez utiliser la commande suivante :
# MAGIC
# MAGIC ```
# MAGIC OPTIMIZE maTable ZORDER BY (col1, col2)
# MAGIC ```
# MAGIC
# MAGIC Cette commande réorganisera les données au sein de la table Delta en fonction des colonnes spécifiées. Elle créera de nouveaux fichiers de données et placera les données qui partagent les mêmes valeurs pour les colonnes spécifiées ensemble. Cela peut améliorer les performances des requêtes lors du filtrage, de la jointure ou de l'agrégation des données basées sur ces colonnes.
# MAGIC
# MAGIC Il est important de noter que le Z-Ordering est le plus efficace lorsqu'il est utilisé avec des colonnes fréquemment utilisées dans les requêtes et ayant une haute cardinalité, c'est-à-dire un grand nombre de valeurs distinctes. De plus, il est recommandé d'utiliser le Z-Ordering sur des colonnes qui sont couramment utilisées ensemble dans les requêtes pour obtenir les meilleurs gains de performance.
# MAGIC
# MAGIC Vous pouvez également utiliser la commande `DESCRIBE DETAIL` sur une table Delta pour voir les informations actuelles sur le Z-Ordering d'une table. Cela vous montrera les colonnes qui ont été utilisées pour le Z-Ordering et le nombre de fichiers de données qui ont été créés en conséquence.

# COMMAND ----------

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
# MAGIC DESCRIBE  HISTORY   delta.`/FileStore/tables/scd2Demo`;

# COMMAND ----------

# MAGIC %md
# MAGIC La commande "OPTIMIZE" dans Delta Lake est utilisée pour optimiser la table en fusionnant les petits fichiers en plus gros fichiers

# COMMAND ----------

# MAGIC %sql 
# MAGIC OPTIMIZE delta.`/FileStore/tables/scd2Demo` ZORDER BY  pk1

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM default.scd2demo;
