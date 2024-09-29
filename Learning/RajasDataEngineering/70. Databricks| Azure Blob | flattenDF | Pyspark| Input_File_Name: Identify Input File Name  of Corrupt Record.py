# Databricks notebook source
dbutils.fs.unmount("/mnt/adls_test")

# COMMAND ----------

storage_account = 'dhgs1dev'
access_key = 'vcYtY190syMqwwCbs1ugmmsgt4AS0txUXE75AiCt6/2ZDqXtFAjZPiF2G1DKSM0afzWSlhGyZjPm+AStrGBfTw=='
container_name = 'gs1ca'

# COMMAND ----------

# MAGIC %md
# MAGIC 17. Databricks & Pyspark: Azure Data Lake Storage Integration with Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method 2: Mount your storage account to your Databricks cluster
# MAGIC
# MAGIC **dbutils.fs.mount**
# MAGIC
# MAGIC In this section, you mount your Azure Data Lake Storage Gen2 cloud object storage to the Databricks File System (DBFS). You use the Azure AD service principle you created previously for authentication with the storage account. For more information, see Mounting cloud object storage on Azure Databricks.
# MAGIC

# COMMAND ----------

configs = {f"fs.azure.account.key.{storage_account}.blob.core.windows.net": access_key}

dbutils.fs.mount(
source = f"wasbs://{container_name}@{storage_account}.blob.core.windows.net/ALF_TEST/",
mount_point = "/mnt/adls_test",
extra_configs = configs)


# COMMAND ----------

root_path = '/mnt/adls_test/Input_File_Name/'

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/adls_test/Input_File_Name/

# COMMAND ----------

from pyspark.sql import functions as F

# for csv 
# df = spark.read.option('sep', ',').option('header', True).csv(root_path)
# display(df)

df = spark.read.json("/mnt/adls_test/Input_File_Name/json/*/")
display(df)

# COMMAND ----------

list_values = ['00075137000070', '00079400456038']
df = df.filter(df.gtin14.isin(list_values))
df = df.withColumns({'input_file_name': F.input_file_name(), 'data_ingested' : F.current_timestamp()})
display(df)

# COMMAND ----------

dbutils.fs.refreshMounts()
# dbutils.fs.unmount("/mnt/adls_test")
