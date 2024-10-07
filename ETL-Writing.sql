-- Databricks notebook source
-- MAGIC %run ./Includes/Copy-Datasets

-- COMMAND ----------

create table orders as
select * from parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("dbfs:/mnt/demo-datasets/bookstore"))

-- COMMAND ----------

select * from orders

-- COMMAND ----------

create or replace table orders as
select * from parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

describe history orders

-- COMMAND ----------

insert overwrite table orders
select * from parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

describe history orders

-- COMMAND ----------

insert into table orders
select * from parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

select count(*) from orders

-- COMMAND ----------

insert overwrite table orders
select * from parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

select count(*) from orders

-- COMMAND ----------

create or replace temp view customers_update as
select * from json.`${dataset.bookstore}/customers-json-new`;

merge into customers c
using customers_update u
on c.customer_id = u.customer_id
when matched and c.email is null and u.email is not null then
  update set email = u.email, updated = u.updated
when not matched then insert *


-- COMMAND ----------

select * from customers

-- COMMAND ----------

create or replace temp view books_updates
(book_id string, title string, author string, category string, price double)
using csv
options (
  path = "${dataset.bookstore}/books-csv-new",
  header = "true",
  delimiter = ";"
);
select * from books_updates 

-- COMMAND ----------

merge into books b
using books_updates u
on b.bookd_id = u.book_id and b.title = u.title
when not matched and u.category = 'Computer Science' then insert *

-- COMMAND ----------

merge into books b
using books_updates u
on b.bookd_id = u.book_id and b.title = u.title
when matched and u.category = 'Computer Science' then 
  delete
when not matched then
  insert (bookd_id, title, author, category, price) 
  values (u.book_id, u.title, u.author, u.category, u.price)


-- COMMAND ----------

select * from books

-- COMMAND ----------


