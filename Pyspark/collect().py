# Databricks notebook source
data = [(1, 'Jack', 'M', 2000,'HR'),(2, 'Asi', 'F', 2000,'HR'),(3, 'Rock', 'M', 2000,'HR'),(4, 'Ayesha', 'F', 2000,'IT'),(5, 'Harley', 'M', 2000,'IT')]
schema = ['id', 'name', 'Gender', 'salary','dep']

df = spark.createDataFrame(data, schema)
df.show()


# COMMAND ----------

dataRow = df.collect()

# COMMAND ----------

print(dataRow)

# COMMAND ----------

print(dataRow[0])

# COMMAND ----------

print(dataRow[0][1])

# COMMAND ----------


