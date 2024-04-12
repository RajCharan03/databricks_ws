# Databricks notebook source
# MAGIC %scala
# MAGIC spark

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.empGlobal

# COMMAND ----------

spark.catalog.listTables('default')


# COMMAND ----------

spark.catalog.listTables('global_temp')

# COMMAND ----------


