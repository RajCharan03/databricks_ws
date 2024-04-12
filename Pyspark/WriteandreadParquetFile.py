# Databricks notebook source
data = [(1, 'Charan'), (2, 'Raj')]
schema = ['id', 'name']

df = spark.createDataFrame(data = data, schema = schema)
display(df)

# COMMAND ----------

df.write.parquet(path = 'dbfs:/FileStore/parquetfile', mode='overwrite')

# COMMAND ----------

display(spark.read.parquet('/FileStore/parquetfile'))

# COMMAND ----------


