# Databricks notebook source
# MAGIC %md
# MAGIC ## **Methods of ADLS Integration Databricks**

# COMMAND ----------

storage_account = 'dhgs1dev'
access_key = 'vcYtY190syMqwwCbs1ugmmsgt4AS0txUXE75AiCt6/2ZDqXtFAjZPiF2G1DKSM0afzWSlhGyZjPm+AStrGBfTw=='
container_name = 'gs1ca'

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method 1: Using DLS access key directly

# COMMAND ----------

spark.conf.set(
    f'fs.azure.account.key.{storage_account}.dfs.core.windows.net',
    access_key
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### ls command (dbutils.fs.ls) from data lake azure 
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/dev-tools/databricks-utils#--file-system-utility-dbutilsfs
# MAGIC
# MAGIC Lists the contents of a directory.
# MAGIC
# MAGIC To display help for this command, run dbutils.fs.help("ls").
# MAGIC
# MAGIC This example displays information about the contents of /tmp. The modificationTime field is available in Databricks Runtime 10.2 and above. In R, modificationTime is returned as a string.

# COMMAND ----------

# dbutils.fs.ls(f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/api=state/date=2024-04-11")
dbutils.fs.ls(f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/api=product/date=2024-04-10/__TEST")


# COMMAND ----------

# MAGIC %md
# MAGIC Show data json state and explode from ADLS

# COMMAND ----------

from pyspark.sql.functions import  explode

# set files location ADLS
files_location = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/api=product/date=2024-04-10/__TEST"

df_state_json = spark.read.json(files_location)
df_state_json.printSchema()
display(df_state_json)

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
source = f"wasbs://{container_name}@{storage_account}.blob.core.windows.net/api=state/",
mount_point = "/mnt/adls_test",
extra_configs = configs)

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/adls_test
# MAGIC

# COMMAND ----------

dbutils.fs.ls('/mnt/adls_test')

# COMMAND ----------

# MAGIC %md
# MAGIC ### refreshMounts command (dbutils.fs.refreshMounts)
# MAGIC Forces all machines in the cluster to refresh their mount cache, ensuring they receive the most recent information.
# MAGIC
# MAGIC To display help for this command, run dbutils.fs.help("refreshMounts").
# MAGIC
# MAGIC

# COMMAND ----------

dbutils.fs.refreshMounts()

# COMMAND ----------

# MAGIC %md
# MAGIC ### unmount command (dbutils.fs.unmount)
# MAGIC Deletes a DBFS mount point.
# MAGIC
# MAGIC  Warning
# MAGIC
# MAGIC To avoid errors, never modify a mount point while other jobs are reading or writing to it. After modifying a mount, always run dbutils.fs.refreshMounts() on all other running clusters to propagate any mount updates. See refreshMounts command (dbutils.fs.refreshMounts).
# MAGIC
# MAGIC

# COMMAND ----------

dbutils.fs.unmount("/mnt/adls_test")

# COMMAND ----------


