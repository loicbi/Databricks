# Databricks notebook source
# MAGIC %md
# MAGIC Returns a merged array of structs in which the nth struct contains all nth values of input arrays.
# MAGIC
# MAGIC
# MAGIC Renvoie un tableau fusionné de structures dans lequel la nième structure contient toutes les nièmes valeurs des tableaux d'entrée.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT arrays_zip(array(1, 2, 3), array(2, 3, 4));
# MAGIC -- [{1,2},{2,3},{3,4}]
# MAGIC
# MAGIC SELECT arrays_zip(array(1, 2), array(2, 3), array(3, 4));
# MAGIC --  [{1,2,3},{2,3,4}]
# MAGIC
# MAGIC SELECT arrays_zip(array(1, 2), array('shoe', 'string', 'budget'));
# MAGIC --  [{1, shoe},{2, string},{null,budget}]
# MAGIC
# MAGIC SELECT arrays_zip();
# MAGIC --  [{}]
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Df 

# COMMAND ----------

from pyspark.sql.functions import arrays_zip, explode
data = [
    ([1, 2, 3], [2, 4, 6], ['aaaa', 'bbb', 888888888888]),\
    ([7, 8, 9], [10, 11, 12], ['cccc', 'ddd', 99999999999]),\
    ([13, 14, 15], [16, 17, 18], ['eee', 'ffff', 444444444444]),\
]
df = spark.createDataFrame(data, ['vals1', 'vals2', 'vals3'])
df.show(10, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Array Zip function on Array Df

# COMMAND ----------

df = df.withColumn('array_zip_col', arrays_zip(df.vals1, df.vals2, df.vals3).alias('zipped'))
df.show(10, False)

# COMMAND ----------

# MAGIC %md
# MAGIC # Pratical Use Case to fallten data using array_zip anb explode 

# COMMAND ----------


data = [
    ('Data Engineer', [
        {"name": "John", "age": 30},
        {"name": "Jane", "age": 26},
        {"name": "Bob", "age": 40}
    ]),
    ('AI Engineer', [
        {"name": "Loic", "age": 30},
        {"name": "Jean", "age": 28}
    ])
]

df = spark.createDataFrame(data=data, schema = ['Title', 'Developers'])
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Apply array_zip 

# COMMAND ----------

df_zip = df.withColumn('zip_col', arrays_zip(df['Developers']))
df_zip.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Apply explode 

# COMMAND ----------

df_zip_explode = df_zip.withColumn('zip_col_explode', explode(df_zip['zip_col'])).drop('Developers')
df_zip_explode.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Faltten fields from exploded list  

# COMMAND ----------

df_zip_explode_flatten = df_zip_explode.withColumns({'Name': df_zip_explode['zip_col_explode.Developers.name'],\
    'Age': df_zip_explode['zip_col_explode.Developers.age']}).drop('zip_col').drop('zip_col_explode')

display(df_zip_explode_flatten)

# COMMAND ----------


