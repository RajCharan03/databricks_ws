# Databricks notebook source
data = [(1, 'Jack', 'M', 2000),(2, 'asi', 'F', 2000), (3,'abcd','', 2000)]
schema = ['id', 'name', 'Gender', 'salary']

df = spark.createDataFrame(data, schema)
df.show()

# COMMAND ----------

from pyspark.sql.functions import when

df1 = df.select(df.id, df.name, when(condition = df.Gender=='M', value = 'Male').when(condition = df.Gender == 'F', value = 'Female').otherwise('unkonwn').alias('Gender'),df.salary)

df1.show()



# COMMAND ----------


