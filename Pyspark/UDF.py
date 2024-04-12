# Databricks notebook source
data = [(1, 'Jack', 200000, 2000),(2, 'Asi', 200000, 2000), (3,'David',200000, 2000)]
schema = ['id', 'name', 'salary', 'bonus']

df = spark.createDataFrame(data, schema)
df.show()

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

def totalPay(s,b):
    return s+b

TotalPayment = udf(lambda s,b:totalPay(s,b), IntegerType())

df.withColumn('totalPay', TotalPayment(df.salary, df.bonus)).show()

# COMMAND ----------

@udf(returnType=IntegerType())
def totalPay(s,b):
    return s+b

# COMMAND ----------

df.select('*', totalPay(df.salary, df.bonus).alias('totalPay')).show()

# COMMAND ----------

df.createOrReplaceTempView('emps')

# COMMAND ----------

from pyspark.sql.types import IntegerType
def totalPay(s,b):
    return s+b

spark.udf.register(name='TotPay', f=totalPay, returnType=IntegerType())

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emps

# COMMAND ----------


