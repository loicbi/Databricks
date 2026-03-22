-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

CREATE TABLE orders AS
SELECT * FROM parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

SELECT * FROM orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Overwriting Tables

-- COMMAND ----------

CREATE OR REPLACE TABLE orders AS
SELECT * FROM parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

DESCRIBE HISTORY orders

-- COMMAND ----------

/*

It looks like you're referencing a SQL command, possibly for use in a Databricks environment. The `INSERT OVERWRITE` statement is commonly used in SQL to overwrite existing data in a table or partition with new data. Here's a basic example of how you might use `INSERT OVERWRITE` in a Databricks notebook with SQL:

```sql
-- Assuming you have a table named 'orders'
-- And you want to overwrite it with new data

INSERT OVERWRITE TABLE orders
SELECT * FROM another_table;
```

*/

INSERT OVERWRITE orders
SELECT * FROM parquet.`${dataset.bookstore}/orders`;

-- COMMAND ----------

DESCRIBE HISTORY orders;

-- COMMAND ----------

INSERT OVERWRITE orders
SELECT *, current_timestamp() FROM parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Appending Data

-- COMMAND ----------

INSERT INTO orders
SELECT * FROM parquet.`${dataset.bookstore}/orders-new`

-- COMMAND ----------

SELECT count(*) FROM orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Merging Data

-- COMMAND ----------

update customers 
-- set profile = '''{"first_name":"Jacquelyn","last_name":"Abby","gender":"Female","address":{"street":"0 Elmside Court","city":"Gornji Milanovac","country":"Serbia"}}'''
-- where customer_id = 'C00601';
-- update customers 
-- set profile = '''{"first_name":"Fred","last_name":"Assogba","gender":"Female","address":{"street":"760 Express Court","city":"Baby","country":"Mali"}}'''
-- where customer_id = 'C01701';


SELECT * FROM customers
where customer_id = 'C01701';

-- check bk 
-- SELECT customer_id, COUNT(*) FROM customers
-- group by customer_id 
-- HAVING COUNT(*) > 1;


-- COMMAND ----------

-- TO AVOID DOUBLON IN TABLE, WE'LL INSERT NEW DATA IN TABLE customers FROM customers-json-new USING MERGE INTO 

CREATE OR REPLACE TEMP VIEW customers_updates AS 
SELECT * FROM json.`${dataset.bookstore}/customers-json-new`;

-- or 
/* 
CREATE OR REPLACE TEMP VIEW customers_updates
(customer_id STRING, email STRING,  profile STRING, updated STRING)
USING JSON
OPTIONS (path = "${dataset.bookstore}/customers-json-new")
;
*/

MERGE INTO customers c
USING customers_updates u
ON c.customer_id = u.customer_id -- bk (customer_id)
WHEN MATCHED AND c.email IS NULL AND u.email IS NOT NULL THEN -- update column email if c.email null value
  UPDATE SET email = u.email, updated = u.updated

WHEN MATCHED AND c.profile <> u.profile THEN -- update column email  if c.profile <> u.profile
  UPDATE SET profile = u.profile, updated = u.updated -- update profile 

WHEN NOT MATCHED THEN 
  INSERT *;


-- before: 1700 
-- after : 1901 

-- num_affected_rows	num_updated_rows	num_deleted_rows	num_inserted_rows
-- 301	100	0	201

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW books_updates
   (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  path = "${dataset.bookstore}/books-csv-new",
  header = "true",
  delimiter = ";"
);

SELECT * FROM books_updates

-- COMMAND ----------

MERGE INTO books b
USING books_updates u
ON b.book_id = u.book_id AND b.title = u.title
WHEN NOT MATCHED AND u.category = 'Computer Science' THEN 
  INSERT *
