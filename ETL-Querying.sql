-- Databricks notebook source
-- MAGIC %run ./Includes/Copy-Datasets

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/customers-json")
-- MAGIC display(files)

-- COMMAND ----------

select * from json.`${dataset.bookstore}/customers-json/export_001.json`

-- COMMAND ----------

select * from json.`${dataset.bookstore}/customers-json/`

-- COMMAND ----------

select *, input_file_name() source_file from json.`${dataset.bookstore}/customers-json/`

-- COMMAND ----------

select * from text.`${dataset.bookstore}/customers-json/`  

-- COMMAND ----------

select * from binaryfile.`${dataset.bookstore}/customers-json/`  

-- COMMAND ----------

select * from csv.`${dataset.bookstore}/books-csv`

-- COMMAND ----------

create table books_csv
(book_id string, title string, author string, category string, price double)
using csv
options (header = "true", delimiter = ";")
location "${dataset.bookstore}/books-csv"

-- COMMAND ----------

select * from books_csv

-- COMMAND ----------

describe extended books_csv 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.read
-- MAGIC   .table("books_csv")  # Ensure the table name is correct and does not use backticks
-- MAGIC   .write
-- MAGIC     .mode("append")
-- MAGIC     .format("csv")
-- MAGIC     .option("header", "true")
-- MAGIC     .option("delimiter", ";")
-- MAGIC     .save(f"{dataset_bookstore}/books-csv"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

select count(*) from books_csv

-- COMMAND ----------

refresh table books_csv

-- COMMAND ----------

select count(*) from books_csv

-- COMMAND ----------

create table customers as 
select * from json.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

describe extended customers

-- COMMAND ----------

create table book_unparsed as
select * from csv.`${dataset.bookstore}/books-csv`

-- COMMAND ----------

describe extended book_unparsed

-- COMMAND ----------

create temp view books_tmp
  (bookd_id string, title string, author string, category string, price double)
  using csv
  options
  (
    path='${dataset.bookstore}/books-csv/*.csv',
    header="true",
    delimiter=";"
  );

  create table books as select * from books_tmp;

-- COMMAND ----------

select * from  books

-- COMMAND ----------

create temp view books_tmp_testing
  using csv
  options
  (
    path='${dataset.bookstore}/books-csv/*.csv',
    header="true",
    delimiter=";"
  );

  create table books_tst as select * from books_tmp_testing;

-- COMMAND ----------

select * from books_tst

-- COMMAND ----------


