# Databricks notebook source
# MAGIC %run ./Includes/Copy-Datasets

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files)

# COMMAND ----------

(spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format","parquet")
        .option("cloudFiles.schemaLocation","dbfs:/mnt/demo/orders_checkpoint")
        .load(f"{dataset_bookstore}/orders-raw")
       .writeStream
        .option("checkpointLocation", f"{dataset_bookstore}/orders-checkpoint")
        .table("orders_updates")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders_updates

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*)  from orders_updates

# COMMAND ----------

load_new_data()

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*)  from orders_updates

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history orders_updates

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table orders_updates

# COMMAND ----------

dbutils.fs.rm('dbfs:/mnt/demo/orders_checkpoint', True)

# COMMAND ----------


