# Databricks notebook source
# MAGIC %run ./Includes/Copy-Datasets

# COMMAND ----------

(spark.readStream
      .table("books")
      .createOrReplaceTempView("books_temp_vw")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from books_temp_vw

# COMMAND ----------

# MAGIC %sql
# MAGIC select author, count(bookd_id) as book_count from books_temp_vw group by author 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from books

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view autho_counts_tmp_vw as (
# MAGIC   select author, count(bookd_id) as total_books 
# MAGIC   from books_temp_vw 
# MAGIC   group by author
# MAGIC )

# COMMAND ----------

(
    spark.table("autho_counts_tmp_vw")
    .writeStream
    .trigger(processingTime="10 seconds")
    .outputMode("complete")
    .option("checkpointLocation", "/tmp/checkpoints/author_counts")
    .table("author_counts")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from author_counts

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into books
# MAGIC values
# MAGIC ("B001","Book1","Neal A","CS",10),
# MAGIC ("B002","Book2","Ken G","Data",20),
# MAGIC ("B003","Book3","Nico B","Photo",30),
# MAGIC ("B004","Book4","Efrein M","Health",40)

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into books
# MAGIC values
# MAGIC ("B00111","Book111","CA1","CS",10),
# MAGIC ("B00221","Book221","MG1","Data",20),
# MAGIC ("B00331","Book331","VB1","Photo",30),
# MAGIC ("B00441","Book441","EM1","Health",40)

# COMMAND ----------

(
    spark.table("autho_counts_tmp_vw")
    .writeStream
    .trigger(availableNow=True)
    .outputMode("complete")
    .option("checkpointLocation", "/tmp/checkpoints/author_counts")
    .table("author_counts")
    .awaitTermination()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from author_counts

# COMMAND ----------


