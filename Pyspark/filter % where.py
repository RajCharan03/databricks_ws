# Databricks notebook source
data = [(1, 'Jack', 'M', 2000),(2, 'Asi', 'F', 2000), (3,'David','M', 2000)]
schema = ['id', 'name', 'Gender', 'salary']

df = spark.createDataFrame(data, schema)
df.show()

# COMMAND ----------

df.where(df.name == 'Jack').show()

# COMMAND ----------

df.filter(df.Gender == 'M').show()

# COMMAND ----------

df.filter("Gender == 'M'").show()

# COMMAND ----------

df.where((df.Gender == 'M') & (df.salary == 2000)).show()

# COMMAND ----------


