# Databricks notebook source
data1 = [(1, 'Jack', 'M', 2000),(2, 'Asi', 'F', 2000)]
schema1 = ['id', 'name', 'Gender', 'salary']

data2 = [(3, 'Rock', 'M', 2000),(4, 'Ayesha', 'F', 2000),(1, 'Jack', 'M', 2000)]
schema2 = ['id', 'name', 'Gender', 'salary']

df1 = spark.createDataFrame(data1, schema1)
df2 = spark.createDataFrame(data2, schema2)
df1.show()
df2.show()

# COMMAND ----------

newDf = df1.union(df2)
newDf.show()

# COMMAND ----------

newDf2 = df1.unionAll(df2)
newDf2.show()

# COMMAND ----------

newDf.distinct().show()

# COMMAND ----------


