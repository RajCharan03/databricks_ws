# Databricks notebook source
data = [(1, 'Jack', 'M', 2000),(2, 'Asi', 'F', 2000), (3,'David','M', 2000)]
schema = ['id', 'name', 'Gender', 'salary']

df = spark.createDataFrame(data, schema)
df.show()


# COMMAND ----------

from pyspark.sql.functions import upper

def convertUpperCase(df):
    return df.withColumn('name', upper(df.name))


def doubleSalary(df):
    return df.withColumn('salary', df.salary * 2)

# COMMAND ----------

df1 = df.transform(convertUpperCase).transform(doubleSalary)

df1.show()

# COMMAND ----------


