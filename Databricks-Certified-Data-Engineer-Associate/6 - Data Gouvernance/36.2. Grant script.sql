-- Databricks notebook source
use catalog hive_metastore;

use hive_metastore.hr_db;

-- COMMAND ----------

GRANT SELECT, READ_METADATA, CREATE ON DATABASE hive_metastore.hr_db TO hr_team;
