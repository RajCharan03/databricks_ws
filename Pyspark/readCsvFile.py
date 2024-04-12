# Databricks notebook source
df = spark.read.csv(path = 'dbfs:/FileStore/circuits.csv', header=True)
display(df)

# COMMAND ----------

df = spark.read.format('csv').option('header', True).load(path =  'dbfs:/FileStore/circuits.csv')
display(df)
df.printSchema()

# COMMAND ----------

from pyspark.sql.types import *

path = 'dbfs:/FileStore/sample/'
schema = StructType([StructField(name = 'circuitId', dataType= IntegerType()),
                     StructField(name = 'circuitRef', dataType = StringType()),
                     StructField(name = 'name', dataType = StringType())])

df = spark.read.format('csv').option('header', True).load(path = path, schema = schema)
display(df)
df.printSchema()

# COMMAND ----------


