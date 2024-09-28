-- Databricks notebook source
CREate table if not exists schemadbck.tbl_smartphones (id int,name string, brand string, year int)

-- COMMAND ----------

insert into schemadbck.tbl_smartphones
values
(1, 'Iphone 14', 'Apple', 2022),
(2, 'Iphone 13', 'Apple', 2023),
(3, 'Iphone 6', 'Apple', 2014),
(4, 'Iphone Air', 'Apple', 2013),
(5, 'Galaxy S22', 'Samsung', 2022),
(6, 'Galaxy Z Fold', 'Samsung', 2022),
(7, 'Galaxy S9', 'Samsung', 2016),
(8, 'I2 Pro', 'Xiaomi', 2022),
(9, 'Redmi 11T Pro', 'Xiaomi', 2022),
(10, 'Redmi Note 11', 'Xiaomi', 2021)

-- COMMAND ----------

show tables

-- COMMAND ----------

use schemadbck

-- COMMAND ----------

show tables

-- COMMAND ----------

create view view_apple_phones
as
select * from tbl_smartphones
where brand = 'Apple'

-- COMMAND ----------

select * from view_apple_phones

-- COMMAND ----------

create temp view temp_view_phonebrands
as
select distinct brand from tbl_smartphones;

 select * from temp_view_phonebrands

-- COMMAND ----------

select * from temp_view_phonebrands

-- COMMAND ----------

show tables

-- COMMAND ----------

create global temp view global_temp_view_latest_phones
as
select * from tbl_smartphones
where year > 2020
order by year desc

-- COMMAND ----------

select * from global_temp.global_temp_view_latest_phones

-- COMMAND ----------

show tables

-- COMMAND ----------

show tables in global_temp

-- COMMAND ----------


