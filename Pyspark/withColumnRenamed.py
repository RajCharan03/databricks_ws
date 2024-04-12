# Databricks notebook source
data = [(1,'jack','3000'), 
        (2,'jhon', '3000')]


columns = ['id', 'name','salary']

df =spark.createDataFrame(data = data, schema = columns)
df.show()

# COMMAND ----------

df1 = df.withColumnRenamed('name', 'Empname')
df1.show()

# COMMAND ----------


