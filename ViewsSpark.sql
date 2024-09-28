-- Databricks notebook source
use schemadbck

-- COMMAND ----------

show tables

-- COMMAND ----------

select * from global_temp.global_temp_view_latest_phones

-- COMMAND ----------

select * from temp_view_phonebrands

-- COMMAND ----------

Drop table tbl_smartphones

-- COMMAND ----------

drop view temp_view_phonebrands

-- COMMAND ----------

show tables in global_temp

-- COMMAND ----------

drop view global_temp.global_temp_view_latest_phones

-- COMMAND ----------


