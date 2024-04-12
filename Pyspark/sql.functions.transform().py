# Databricks notebook source
data = [(1, 'jack', ['azure', 'sql']), (2, 'david', ['aws', 'python'])]

schema = ['id', 'name', 'skills']

df = spark.createDataFrame(data, schema)
df.show()

# COMMAND ----------

from pyspark.sql.functions import transform, upper

def conUpperCase(x):
    return upper(x)

# COMMAND ----------

df.select('id', 'name', transform('skills', conUpperCase).alias('skills')).show()

# COMMAND ----------

df.select('id', transform('skills', lambda x: upper(x)).alias('skills')).show()

# COMMAND ----------


