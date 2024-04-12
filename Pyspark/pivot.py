# Databricks notebook source
data = [(1, 'Jack', 'M', 2000,'HR'),(2, 'Asi', 'F', 3000,'HR'),(3, 'Rock', 'M', 2000,'HR'),(4, 'Ayesha', 'F', 3000,'IT'),(5, 'Harley', 'M', 5000,'IT')]
schema = ['id', 'name', 'gender', 'salary','dep']

df = spark.createDataFrame(data, schema)
df.show()

# COMMAND ----------

df.groupBy('dep', 'gender').count().show()

# COMMAND ----------

df.groupBy('dep').pivot('gender').count().show()

# COMMAND ----------

df.groupBy('dep').pivot('gender',['M']).count().show()

# COMMAND ----------


