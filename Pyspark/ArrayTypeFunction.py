# Databricks notebook source
## explode
data = [(1, 'jack', ['dot', 'java']),(2, 'david', ['azure', 'sql'])]

schema = ['id', 'name', 'skills']

df = spark.createDataFrame(data, schema)
df.show()
df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import explode, col
df.show()
df1 = df.withColumn('skill', explode(col('skills')))
df1.show()

# COMMAND ----------

## split
data = [(1, 'jack', 'dot,java,sql'),(2, 'david', 'azure,sql,python')]

schema = ['id', 'name', 'skills']

df2 = spark.createDataFrame(data, schema)
df2.show()
df2.printSchema()

# COMMAND ----------

from pyspark.sql.functions import split,col

df2.show()

df3 = df2.withColumn('skillsArray', split(col('skills'), ','))
df3.show()

# COMMAND ----------

## Array
data = [(1, 'jack', 'dot','java'),(2, 'david', 'azure','sql')]

schema = ['id', 'name', 'Pskills', 'Sskills']

df4 = spark.createDataFrame(data, schema)
df4.show()
df4.printSchema()

# COMMAND ----------

from pyspark.sql.functions import array,col

df4.show()
df5 = df4.withColumn('skills', array(col('Pskills'), col('Sskills')))
df5.show()
df5.printSchema()

# COMMAND ----------

## Arraycontains
data = [(1, 'jack', ['dot', 'java']),(2, 'david', ['azure', 'sql'])]

schema = ['id', 'name', 'skills']

df6 = spark.createDataFrame(data, schema)
df6.show()
df6.printSchema()

# COMMAND ----------

from pyspark.sql.functions import array_contains, col

df6.show()
df7 = df6.withColumn('hasJavaSkill', array_contains(col('skills'), 'java'))
df7.show()

# COMMAND ----------


