# Databricks notebook source
data = [('IT',8,5),\
        ('PayRoll',3,2),\
        ('HR',2,4)]
schema = ['dep', 'male', 'female']

df = spark.createDataFrame(data, schema)
df.show()

# COMMAND ----------

from pyspark.sql.functions import expr

unpivotdf = df.select('dep', expr("stack(2,'male', male, 'female', female) as (gender, count)"))

df.show()
unpivotdf.show()

# COMMAND ----------


