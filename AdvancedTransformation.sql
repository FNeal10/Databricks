-- Databricks notebook source
-- MAGIC %run ./Includes/Copy-Datasets

-- COMMAND ----------

select * from customers

-- COMMAND ----------

select customers.customer_id, profile:first_name, profile:address.country
from customers

-- COMMAND ----------

select * from orders

-- COMMAND ----------

select orders.order_id, customer_id, explode(books) as book
from orders

-- COMMAND ----------

select c 
from orders
