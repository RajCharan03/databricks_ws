# Databricks notebook source
from pyspark.sql.functions import lit,col

data = [('Harley', 'male', 25), ('David', 'male', 26)]
schema = ['name', 'gender','age']

df = spark.createDataFrame(data, schema)
df.show()

# COMMAND ----------

df1 = df.withColumn('newCol', lit('value'))
df1.show()

# COMMAND ----------

df.select(df.name).show()
df.select(df['gender']).show()
df.select(col('name')).show()

# COMMAND ----------

from pyspark.sql.types import StructField,StructType,StringType,IntegerType
data = [('Harley', 'male', 2500,('black', 'brown')), ('David', 'male', 2600,('black', 'blue'))]

propType = StructType([StructField('hair',StringType()), StructField('eye', StringType())])

schema = StructType([StructField('name', StringType()),\
                    StructField('gender', StringType()),\
                    StructField('salary', IntegerType()),\
                    StructField('props', propType)])

df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

df.select(df.props.hair).show()
df.select(df.props.eye).show()

# COMMAND ----------


