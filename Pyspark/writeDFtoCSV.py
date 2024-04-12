# Databricks notebook source
from pyspark.sql import *


# COMMAND ----------

data = [(1, 'Charan'), (2, 'Raj')]
schema = ['id', 'name']

df = spark.createDataFrame(data = data, schema = schema)
display(df)

# COMMAND ----------

df.write.csv(path = 'dbfs:/temp/emp', header='True')

# COMMAND ----------

df1 = spark.read.csv(path = 'dbfs:/temp/emp', header='True')
display(df1)

# COMMAND ----------

help(df.write.csv)

#  mode : str, optional
#     specifies the behavior of the save operation when data already exists.
    
#         * ``append``: Append contents of this :class:`DataFrame` to existing data.
#         * ``overwrite``: Overwrite existing data.
#         * ``ignore``: Silently ignore this operation if data already exists.
#         * ``error`` or ``errorifexists`` (default case): Throw an exception if data already \
#             exists.


# COMMAND ----------

df.write.csv(path = 'dbfs:/temp/emp', header='True', mode = 'overwrite')
df = spark.read.csv(path = 'dbfs:/temp/emp', header='True')
display(df)

# COMMAND ----------

df.write.csv(path = 'dbfs:/temp/emp', header='True', mode = 'overwrite')
df = spark.read.csv(path = 'dbfs:/temp/emp', header='True')
display(df)

# COMMAND ----------

df.write.csv(path = 'dbfs:/temp/emp', header='True', mode = 'ignore')
df = spark.read.csv(path = 'dbfs:/temp/emp', header='True')
display(df)

# COMMAND ----------

df.write.csv(path = 'dbfs:/temp/emp', header='True', mode = 'error')
df = spark.read.csv(path = 'dbfs:/temp/emp', header='True')
display(df)

# COMMAND ----------


