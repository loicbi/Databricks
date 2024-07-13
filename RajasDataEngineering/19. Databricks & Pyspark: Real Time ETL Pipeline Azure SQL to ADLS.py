# Databricks notebook source
storage_account = 'dhgs1dev'
access_key = 'vcYtY190syMqwwCbs1ugmmsgt4AS0txUXE75AiCt6/2ZDqXtFAjZPiF2G1DKSM0afzWSlhGyZjPm+AStrGBfTw=='
container_name = 'gs1ca'

# COMMAND ----------

spark.conf.set(
    f'fs.azure.account.key.{storage_account}.dfs.core.windows.net',
    access_key
)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 1: Extract Data from ADLS 

# COMMAND ----------

from pyspark.sql.functions import  explode

# set files location ADLS
files_location = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/api=state/"

df_state_json = spark.read.json(files_location)
# df_state_json.select('*', explode('*')).show()
df_state_json.printSchema()
display(df_state_json)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2: Show data json state and explode from ADLS

# COMMAND ----------

from pyspark.sql.functions import  explode
import json
# set files location ADLS
files_location = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/__ALF__/"
df_product_json = spark.read.option('multiline', 'true').json(files_location)
# df_product_json.printSchema()  
display(df_product_json)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3: Load transformed Data into ADLS
# MAGIC Create Mount Point for ADLS Integration

# COMMAND ----------

configs = {f"fs.azure.account.key.{storage_account}.blob.core.windows.net": access_key}


dbutils.fs.mount(
source = f"wasbs://{container_name}@{storage_account}.blob.core.windows.net/__ALF__",
mount_point = "/mnt/adls_test",
extra_configs = configs)


# COMMAND ----------

# MAGIC %md
# MAGIC Write the Data in Parquet Format 

# COMMAND ----------

df_product_json.write.format('parquet').mode('overwrite').save('/mnt/adls_test/adv_work_json/')
dbutils.fs.refreshMounts()
dbutils.fs.ls("/mnt/adls_test")

# COMMAND ----------

# MAGIC %md
# MAGIC Write the Data in CSV Format 

# COMMAND ----------

df_product_json.write.format('csv').mode('overwrite').option("header", "true").save('/mnt/adls_test/adv_work_csv/')
dbutils.fs.refreshMounts()
# IF THERE IS A PROBLEM BECAUSE MY FILE IS A JSON FORMAT 

# COMMAND ----------

# MAGIC %md
# MAGIC Flatten data 

# COMMAND ----------

from pyspark.sql import functions as f
from pyspark.sql.types import *



def flatten_structs(nested_df):
    stack = [((), nested_df)]
    columns = []

    while len(stack) > 0:

        parents, df = stack.pop()

        array_cols = [
            c[0]
            for c in df.dtypes
            if c[1][:5] == "array"
        ]

        flat_cols = [
            f.col(".".join(parents + (c[0],))).alias("_".join(parents + (c[0],)))
            for c in df.dtypes
            if c[1][:6] != "struct"
        ]

        nested_cols = [
            c[0]
            for c in df.dtypes
            if c[1][:6] == "struct"
        ]

        columns.extend(flat_cols)

        for nested_col in nested_cols:
            projected_df = df.select(nested_col + ".*")
            stack.append((parents + (nested_col,), projected_df))

    return nested_df.select(columns)

def flatten_array_struct_df(df):

    array_cols = [
            c[0]
            for c in df.dtypes
            if c[1][:5] == "array"
        ]


    while len(array_cols) > 0:

        for array_col in array_cols:
            # check if array contains structure then flatten
            if isinstance(df.schema[array_col].dataType.elementType, StructType) is True:

                cols_to_select = [x for x in df.columns if x != array_col ]

                df = df.withColumn(array_col, f.explode(f.col(array_col)))

        df = flatten_structs(df)

        array_cols = [
            c[0]
            for c in df.dtypes
            if c[1][:5] == "array"
        ]
        return df


# get columns list from dataFrame  
list_column_ims = list(df_product_json.columns)



# Display the column types
def get_column_type_from_df (df) -> list:
    # Get the column types
    column_types = df.dtypes
    # for column_name, data_type in column_types:
    #     print(f"Column '{column_name}' has data type: {data_type}")
    return [data_type for data_type in column_types]



for col in list_column_ims:
    df_col = df_product_json.select(col)

    list_type = get_column_type_from_df(df_col)

    if col == 'identification':
        df_col = flatten_structs(df_col)
        df_col = flatten_array_struct_df(df_col)
        df_col = flatten_array_struct_df(df_col)
        df_col = flatten_array_struct_df(df_col)

    if col == 'marketingContent':
        df_col = flatten_array_struct_df(df_col)
        df_col = flatten_array_struct_df(df_col)
    if col == 'planoContent':
        df_col = flatten_array_struct_df(df_col)
        df_col = flatten_array_struct_df(df_col)
    if col == 'newItemSetup':
        df_col = flatten_array_struct_df(df_col)
        df_col = flatten_array_struct_df(df_col)
    if col == 'ecommerceContent':
        df_col = flatten_array_struct_df(df_col)
        df_col = flatten_array_struct_df(df_col)
    display(df_col)


# df_fl = flatten_array_struct_df(df_product_json)
# display(df_fl)


# COMMAND ----------

# MAGIC %md
# MAGIC Delete File if finish

# COMMAND ----------


# dELETE the DBFS root  
dbutils.fs.unmount("/mnt/adls_test")

# COMMAND ----------


