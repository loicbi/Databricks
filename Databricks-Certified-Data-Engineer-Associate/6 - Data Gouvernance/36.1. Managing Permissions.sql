-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS hive_metastore.hr_db 
LOCATION 'dbfs:/mnt/demo/hr_db.db'; 

-- COMMAND ----------

USE hive_metastore.hr_db;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS employees (id INT , name STRING, salary DOUBLE, city STRING);

-- COMMAND ----------

INSERT INTO employees VALUES 
(1, 'Alice',  2500, 'Paris'),
(2, 'Thomas',  3000, 'London'),
(3, 'Bilal',  3500, 'Paris'),
(4, 'Maya',  2000, 'Paris'),
(5, 'Sophie',  2500, 'London'),
(6, 'Adam',  3500, 'London'),
(7, 'Ali',  3500, 'London');

-- COMMAND ----------

CREATE VIEW v_paris_employees as SELECT * FROM employees WHERE city = 'Paris';
