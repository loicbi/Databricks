# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
# MAGIC </div>

# COMMAND ----------

dbutils.fs.rm('dbfs:/mnt/demo/author_counts_checkpoint', recurse=True)

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Reading Stream

# COMMAND ----------

spark.readStream \
      .table("books") \
      .createOrReplaceTempView("books_streaming_tmp_vw")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Displaying Streaming Data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM books_streaming_tmp_vw

# COMMAND ----------

# MAGIC %md
# MAGIC ## Applying Transformations

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT author, count(book_id) AS total_books
# MAGIC FROM books_streaming_tmp_vw
# MAGIC GROUP BY author

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Unsupported Operations

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT * 
# MAGIC  FROM books_streaming_tmp_vw
# MAGIC  ORDER BY author

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Persisting Streaming Data

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW author_counts_tmp_vw AS (
# MAGIC   SELECT author, count(book_id) AS total_books
# MAGIC   FROM books_streaming_tmp_vw
# MAGIC   GROUP BY author
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM author_counts_tmp_vw ORDER BY author;

# COMMAND ----------

(spark.table("author_counts_tmp_vw")                               
      .writeStream  
      .trigger(processingTime='10 seconds')
      .outputMode("complete")
      .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint")
      .table("author_counts")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM author_counts

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adding New Data

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books
# MAGIC values ("B19", "Introduction to Modeling and Simulation", "Mark W. Spong", "Computer Science", 25),
# MAGIC         ("B20", "Robot Modeling and Control", "Mark W. Spong", "Computer Science", 30),
# MAGIC         ("B21", "Turing's Vision: The Birth of Computer Science", "Chris Bernhardt", "Computer Science", 35)
# MAGIC         

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming in Batch Mode 

# COMMAND ----------

# MAGIC %md
# MAGIC The outputMode("complete") in a streaming query specifies that the entire updated Result Table will be outputted to the sink after every trigger. This mode is typically used for aggregations where the result of the aggregation changes over time and you want to output the complete aggregated results after each trigger.
# MAGIC
# MAGIC However, it's important to note that using outputMode("complete") with non-aggregation queries in a streaming context might not be supported or could lead to unexpected behavior. For instance, if you're writing a stream to a Delta table without any aggregation, you might want to use outputMode("append") instead, which adds new rows to the Result Table and is more suitable for most non-aggregation streaming workloads .

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books
# MAGIC values ("B16", "Hands-On Deep Learning Algorithms with Python", "Sudharsan Ravichandiran", "Computer Science", 25),
# MAGIC         ("B17", "Neural Network Methods in Natural Language Processing", "Yoav Goldberg", "Computer Science", 30),
# MAGIC         ("B18", "Understanding digital signal processing", "Richard Lyons", "Computer Science", 35)

# COMMAND ----------

(spark.table("author_counts_tmp_vw")                               
      .writeStream           
      .trigger(availableNow=True)
      .outputMode("complete")
      .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint")
      .table("author_counts")
      .awaitTermination()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM author_counts
