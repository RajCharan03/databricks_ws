# Databricks notebook source
df = spark.read.json(path = 'dbfs:/FileStore/Jsondata/qualifying_split_1.json', multiLine='True')
df.printSchema()
display(df)


# COMMAND ----------

df1 = spark.read.json(path = 'dbfs:/FileStore/Jsondata/*.json' ,multiLine='True')
df.printSchema()
display(df1)

# COMMAND ----------

df2 = spark.read.json(path = 'dbfs:/FileStore/Jsondata/*' ,multiLine='True')
df.printSchema()
display(df2)

# COMMAND ----------


