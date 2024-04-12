# Databricks notebook source
data1 = [(1, 'Jack', '31'),(2, 'Asi', '29')]
schema1 = ['id', 'name', 'age']

data2 = [(3, 'Rock', 2000),(4, 'Ayesha',  2000)]
schema2 = ['id', 'name', 'salary']

df1 = spark.createDataFrame(data1, schema1)
df2 = spark.createDataFrame(data2, schema2)
df1.show()
df2.show()

# COMMAND ----------

df1.unionByName(df2,allowMissingColumns=True).show()

# COMMAND ----------


