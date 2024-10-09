-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Parsing JSON Data

-- COMMAND ----------

SELECT * FROM customers

-- COMMAND ----------

DESCRIBE customers

-- COMMAND ----------

SELECT customer_id, profile:first_name, profile:address:country 
FROM customers

-- COMMAND ----------

SELECT from_json(profile) AS profile_struct
  FROM customers;

/*

  from_json(jsonStr, schema[, options])

  > SELECT from_json('{"a":1, "b":0.8}', 'a INT, b DOUBLE');
 {1,0.8}

> SELECT from_json('{"time":"26/08/2015"}', 'time Timestamp', map('timestampFormat', 'dd/MM/yyyy'));
 {2015-08-26 00:00:00}


*/

-- COMMAND ----------


 SELECT map(1.0, '2', 3.0, '4');
 -- {1.0 -> 2, 3.0 -> 4}
 
 
-- SELECT from_json('{"time":"26/08/2015"}', 'time Timestamp', map('timestampFormat', 'dd/MM/yyyy'));



-- COMMAND ----------

SELECT profile 
FROM customers 
LIMIT 1

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW parsed_customers AS
  SELECT customer_id, from_json(profile, schema_of_json('{"first_name":"FREDDY","last_name":"ASSOGBA","gender":"M","address":{"street":"999 Av Rue St Andre","city":"Montreal","country":"Canada"}}')) AS profile_struct
  FROM customers;
  
SELECT * FROM parsed_customers;

-- COMMAND ----------

DESCRIBE parsed_customers

-- COMMAND ----------

SELECT customer_id, profile_struct.first_name, profile_struct.address.country
FROM parsed_customers

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW customers_final AS
  SELECT customer_id, profile_struct.*
  FROM parsed_customers;
  
SELECT * FROM customers_final

-- COMMAND ----------

SELECT order_id, customer_id, books
FROM orders

-- COMMAND ----------

DESC orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Explode Function

-- COMMAND ----------

SELECT explode(array(5,5,5,25,10,10))

-- COMMAND ----------

SELECT order_id, customer_id, explode(books) AS book -- EXPLODE USED IN ARRAY DATA TYPE COLUMN
FROM orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Collecting Rows

-- COMMAND ----------

SELECT customer_id
FROM orders

-- COMMAND ----------

SELECT customer_id,
  collect_set(order_id) AS orders_set,
  collect_set(books.book_id) AS books_set
FROM orders
GROUP BY customer_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ##Flatten Arrays

-- COMMAND ----------

SELECT array_distinct(array(5,5,5,25,10,10))

-- COMMAND ----------

SELECT customer_id,
  collect_set(books.book_id) As before_flatten,
  array_distinct(flatten(collect_set(books.book_id))) AS after_flatten
FROM orders
GROUP BY customer_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ##Join Operations

-- COMMAND ----------

CREATE OR REPLACE VIEW orders_enriched AS
SELECT *
FROM (
  SELECT *, explode(books) AS book 
  FROM orders) o
INNER JOIN books b
ON o.book.book_id = b.book_id;

SELECT * FROM orders_enriched

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Set Operations

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW orders_updates
AS SELECT * FROM parquet.`${dataset.bookstore}/orders-new`;

SELECT * FROM orders 
UNION 
SELECT * FROM orders_updates 

-- COMMAND ----------

SELECT * FROM orders 
INTERSECT 
SELECT * FROM orders_updates 

-- COMMAND ----------

SELECT * FROM orders 
MINUS 
SELECT * FROM orders_updates 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Reshaping Data with Pivot

-- COMMAND ----------

CREATE OR REPLACE TABLE transactions AS

SELECT * FROM (
  SELECT
    customer_id,
    book.book_id AS book_id,
    book.quantity AS quantity
  FROM orders_enriched
) PIVOT (
  sum(quantity) FOR book_id in (
    'B01', 'B02', 'B03', 'B04', 'B05', 'B06',
    'B07', 'B08', 'B09', 'B10', 'B11', 'B12'
  )
);

SELECT * FROM transactions
