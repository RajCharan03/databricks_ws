# Databricks notebook source
data = [(1, 'Jack', 'M', 2000),(2, 'Asi', 'F', 2000), (3,'David','M', 2000),(2, 'Asi', 'F', 2000)]
schema = ['id', 'name', 'Gender', 'salary']

df = spark.createDataFrame(data, schema)
df.show()

# COMMAND ----------

df.distinct().show()

# COMMAND ----------

help(df.distinct)

# COMMAND ----------

df.dropDuplicates(['Gender']).show()

# COMMAND ----------

help(df.dropDuplicates)

# COMMAND ----------


