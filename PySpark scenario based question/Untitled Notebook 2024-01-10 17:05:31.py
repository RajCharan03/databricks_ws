# Databricks notebook source
df = spark.read.parquet('/FileStore/for test/')

# COMMAND ----------

df.createOrReplaceTempView('testtb')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select sum(pos_unit_sales_qty), sum(pos_sales_amt) from testtb

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from testtb

# COMMAND ----------


