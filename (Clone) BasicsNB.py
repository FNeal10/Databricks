# Databricks notebook source
# MAGIC %md
# MAGIC Ordered list
# MAGIC 1. 1
# MAGIC 2. 3
# MAGIC 3. 3

# COMMAND ----------

# MAGIC %run ./Includes/Setup

# COMMAND ----------

print(full_name)

# COMMAND ----------

# MAGIC %fs ls 'databricks-datasets'

# COMMAND ----------

dbutils.help()

# COMMAND ----------

files = dbutils.fs.ls('/databricks-datasets/')

# COMMAND ----------

print(files)

# COMMAND ----------

display(files)

# COMMAND ----------


