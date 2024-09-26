-- Databricks notebook source
CREATE TABLE employees (
  id INT,
  employeeName STRING,
  salary DOUBLE
);

-- COMMAND ----------

INSERT into employees
VALUES
(1,'Ken',10),
(2,'Neal',20),
(3,'Nico',30),
(4,'Ren',40)

-- COMMAND ----------

select * from employees

-- COMMAND ----------

DESCRIBE DETAIL demoworkspace.default.employees;

-- COMMAND ----------

INSERT into employees
VALUES
(5,'Mansh',100),
(6,'Pot',200),
(7,'Vera',300),
(8,'Rio',400),
(9,'Luke',500)

-- COMMAND ----------

-- MAGIC %fs ls 'abfss://unity-catalog-storage@dbstoragenogia45666ahw.dfs.core.windows.net/2716506332254685/__unitystorage/catalogs/1189029b-1f3b-46a1-bfa0-5de9cb9d71d7/tables/465be9ba-b1e2-470a-a324-7be0daa57de7'

-- COMMAND ----------

-- MAGIC %fs ls

-- COMMAND ----------

UPDATE employees
SET salary = salary + 500
WHERE employeeName like 'N%'

-- COMMAND ----------

describe history employees

-- COMMAND ----------

delete from employees

-- COMMAND ----------

select * from employees

-- COMMAND ----------

restore table employees version as of 4

-- COMMAND ----------

select * from employees

-- COMMAND ----------

delete from employees where salary >= 100

-- COMMAND ----------

select * from  employees

-- COMMAND ----------

describe history employees

-- COMMAND ----------

restore table employees version as of 6

-- COMMAND ----------

select * from employees

-- COMMAND ----------

describe detail employees

-- COMMAND ----------

optimize employees
zorder by id

-- COMMAND ----------

describe detail employees

-- COMMAND ----------

describe history employees

-- COMMAND ----------

select * from employees

-- COMMAND ----------

vacuum employees

-- COMMAND ----------

drop table employees

-- COMMAND ----------

select * from employees

-- COMMAND ----------

CREATE TABLE schemadbck.tbl_employees (id INT, name STRING, age INT, salary  DOUBLE)

-- COMMAND ----------


