# Databricks notebook source
data = [(1, 'Jack', 'M', 2000,'HR'),(2, 'Asi', 'F', 2000,'IT'), (3,'David','M', 2000,'HR'),(4, 'Harley', 'F', 2000,'IT') ]
schema = ['id', 'name', 'Gender', 'salary','dep']

df = spark.createDataFrame(data, schema)
df.show()

# COMMAND ----------

df.sort(df.id.desc()).show()

# COMMAND ----------

df.orderBy('name').show()

# COMMAND ----------

df.sort(df.dep.desc(), df.id.desc()).show()

# COMMAND ----------

df.orderBy(df.id.desc(), df.dep.desc()).show()

# COMMAND ----------

df.orderBy(df.dep.desc(), df.id.desc()).show()

# COMMAND ----------


