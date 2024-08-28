# Databricks notebook source
# MAGIC %md
# MAGIC La commande "MERGESCHEMA" dans Databricks est utilisée pour fusionner deux ou plusieurs schémas dans Apache Spark. Elle vous permet de combiner plusieurs schémas en un seul schéma en fusionnant leurs champs.
# MAGIC
# MAGIC Lorsque vous avez plusieurs sources de données avec des schémas différents, vous pouvez utiliser la commande MERGESCHEMA pour créer un schéma unifié qui peut être utilisé pour lire et traiter les données. Cela est particulièrement utile lorsque vous travaillez avec des données structurées dans Spark, car cela vous permet de gérer facilement des données avec des structures de schéma variables.
# MAGIC
# MAGIC Pour utiliser la commande MERGESCHEMA dans Databricks, vous devez fournir les schémas que vous souhaitez fusionner en tant qu'entrée. La commande combine ensuite ces schémas et crée un nouveau schéma qui inclut tous les champs des schémas d'entrée.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Table Creation 

# COMMAND ----------

# The second argument, True, specifies that the deletion should be recursive, meaning that it will delete all files and subdirectories within the specified directory.
dbutils.fs.rm('/FileStore/tables/delta/path_emplyee_demo', True)

# COMMAND ----------

from delta.tables import * 
DeltaTable.createOrReplace(spark)\
    .tableName('employee_demo')\
        .addColumn('emp_id', 'INT')\
        .addColumn('emp_name', 'STRING')\
        .addColumn('gender', 'STRING')\
        .addColumn('salary', 'INT')\
        .addColumn('Dept', 'STRING')\
        .property('description', 'table created for demo purpose')\
        .location('/FileStore/tables/delta/path_emplyee_demo')\
        .execute()


# COMMAND ----------

# MAGIC %sql 
# MAGIC INSERT INTO default.employee_demo VALUES(100, 'Stephen', 'M', 2000, 'IT');
# MAGIC SELECT * FROM default.employee_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Evolution 

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType

employee_data  = [(200, 'Philipp', 'M', 8000, 'HR', 'test data')]

employee_schema = StructType([
  StructField('emp_id', IntegerType(), False),
  StructField('emp_name', StringType(), True),
  StructField('gender', StringType(), True),
  StructField('salary', IntegerType(), True),
  StructField('dept', StringType(), True),
  StructField('additionnal_column1', StringType(), True),]
)

df = spark.createDataFrame(data=employee_data, schema=employee_schema, )
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Save df as a table 

# COMMAND ----------

df.write.format('delta').mode('append').saveAsTable('default.employee_demo')

# error because df contains more columns than table default.employee_demo so we'll use merger schema to True 


# COMMAND ----------

df.write.option("mergeSchema", "true").mode("append").saveAsTable('default.employee_demo')

df.printSchema()

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM default.employee_demo;

# COMMAND ----------


