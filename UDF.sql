-- Databricks notebook source
-- MAGIC %run ./Includes/Copy-Datasets

-- COMMAND ----------

select * from orders

-- COMMAND ----------

select order_id,
books,
filter(books, i -> i.quantity >= 2) as mult_copies
from orders

-- COMMAND ----------

select order_id, mult_copies
from (
select order_id,
books,
filter(books, i -> i.quantity >= 2) as mult_copies
from orders
)
where size(mult_copies) > 0

-- COMMAND ----------

select order_id,
books,
transform (
  books,
  b -> cast(b.subtotal * 0.8 as int)
) as subtotal_after_discount
from orders


-- COMMAND ----------

create or replace function get_url(email string)
returns string

return concat("https://www.",split(email,"@")[0])

-- COMMAND ----------

 select email, get_url(email) from customers

-- COMMAND ----------

describe function get_url

-- COMMAND ----------

drop function get_url

-- COMMAND ----------


