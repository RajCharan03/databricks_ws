# Databricks notebook source
data = [(1, 'Jack', 'M', 2000,None),(2, 'Asi', 'F', 2000,'HR'),(3, 'Rock', None, 2000,'HR'),(4, 'Ayesha', 'F', 2000,'IT'),(5, 'Harley', 'M', 2000,'IT')]
schema = ['id', 'name', 'Gender', 'salary','dep']

df = spark.createDataFrame(data, schema)
df.show()

# COMMAND ----------

df.fillna('unknown').show()

# COMMAND ----------

df.fillna('unknown',['gender']).show()

# COMMAND ----------

df.fillna('unknown',['gender','dep']).show()

# COMMAND ----------

df.na.fill('unknown',['gender']).show()

# COMMAND ----------


