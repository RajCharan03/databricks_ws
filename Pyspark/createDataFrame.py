# Databricks notebook source
type(spark)

# COMMAND ----------

dir(spark)

# COMMAND ----------

help(spark.createDataFrame)

# COMMAND ----------

from pyspark.sql.types import *
data = [(1,'Charan'),(2, 'Raj')]

structType([])
schema = ['id','name']

df = spark.createDataFrame(data= data, schema = schema)
df.show()
df.printSchema()

# COMMAND ----------

#create data frame using list and tupels 
from pyspark.sql.types import *

data = [(1, 'Charan'), (2,'Raj'),(3, 'yv')]
schema = StructType([StructField(name = 'id', dataType= IntegerType(), nullable=False),
                     StructField(name = 'name', dataType=StringType(), nullable=False)])

df  = spark.createDataFrame(data = data, schema=schema)
df.show()
df.printSchema()

# COMMAND ----------

# create data frame using list and dict

data = [{'id':1, 'name':'Charan'},
        {'id':2, 'name':'Raj'}]

schema = StructType([StructField(name = 'id', dataType= IntegerType(), nullable=False),
                     StructField(name = 'name', dataType=StringType(), nullable=False)])

df = spark.createDataFrame(data= data, schema = schema)
df.show()
df.printSchema()

# COMMAND ----------



# Sample data as a list of dictionaries
data = [
    {"name": "Alice", "age": 30},
    {"name": "Bob", "age": 25},
    {"name": "Charlie", "age": 35}
]
df = spark.createDataFrame(data)


# Show the DataFrame
df.show()
df.printSchema()


# COMMAND ----------


