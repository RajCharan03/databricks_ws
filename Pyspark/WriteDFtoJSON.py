# Databricks notebook source
data = [(1,'charan'), (2,'raj')]
schema = ['id', 'name']

df  = spark.createDataFrame(data = data, schema = schema)
display(df)

# COMMAND ----------

df.write.json(path= 'dbfs:/temp/jsonfolder')


# COMMAND ----------


df1 = spark.read.json(path = 'dbfs:/temp/jsonfolder')
display(df1)

# COMMAND ----------

df.write.json(path= 'dbfs:/temp/jsonfolder', mode = 'overwrite') 

# COMMAND ----------

df1 = spark.read.json(path = 'dbfs:/temp/jsonfolder')
display(df1)

# COMMAND ----------


