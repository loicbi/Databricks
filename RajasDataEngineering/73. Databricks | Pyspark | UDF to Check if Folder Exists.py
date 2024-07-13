# Databricks notebook source
# MAGIC %md
# MAGIC ## **Methods of ADLS Integration Databricks**

# COMMAND ----------

dbutils.fs.unmount("/mnt/adls_test")

# COMMAND ----------

storage_account = 'dhgs1dev'
access_key = 'vcYtY190syMqwwCbs1ugmmsgt4AS0txUXE75AiCt6/2ZDqXtFAjZPiF2G1DKSM0afzWSlhGyZjPm+AStrGBfTw=='
container_name = 'gs1ca'

# COMMAND ----------

configs = {f"fs.azure.account.key.{storage_account}.blob.core.windows.net": access_key}
dbutils.fs.mount(
source = f"wasbs://{container_name}@{storage_account}.blob.core.windows.net/ALF_TEST/Input_File_Name/json",
mount_point = "/mnt/adls_test/",
extra_configs = configs)

# COMMAND ----------

# MAGIC %fs ls '/mnt/adls_test'
