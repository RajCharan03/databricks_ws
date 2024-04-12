# Databricks notebook source
data = [(1, 'Jack', 'M', 2000,'HR'),(2, 'Asi', 'F', 3000,'HR'),(3, 'Rock', 'M', 2000,'HR'),(4, 'Ayesha', 'F', 3000,'IT'),(5, 'Harley', 'M', 5000,'IT')]
schema = ['id', 'name', 'gender', 'salary','dep']

df = spark.createDataFrame(data, schema)
df.show()

# COMMAND ----------

df1 = df.groupBy('dep').min('salary')
df1.show()

# COMMAND ----------

from pyspark.sql.functions import count, min, max
df.groupBy('dep').agg(count('*').alias('countOfEmp'),\
                    min('salary').alias('minSal'), \
                        max('salary').alias('maxSalary')).show()

# COMMAND ----------


