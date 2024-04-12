# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, MapType
data = [('Harley', {'hair':'black', 'eye':'brown'}),
        ('John',{'hair':'black', 'eye':'blue'})]

schema = StructType([StructField('name', StringType()),\
                     StructField('traits', MapType(StringType(), StringType()))])

df = spark.createDataFrame(data, schema)
display(df)
df.printSchema()


# COMMAND ----------

display(df.withColumn('hair', df.traits['hair']))

# COMMAND ----------

display(df.withColumn('eye', df.traits.getItem('eye')))

# COMMAND ----------

from pyspark.sql.functions import explode, map_keys, map_values,col
display(df.select('name', 'traits', explode(col('traits'))))

# COMMAND ----------

df.select('name', 'traits', map_keys(col('traits'))).show(truncate=False)

# COMMAND ----------

df.select('name', 'traits', map_values(df.traits)).show(truncate=False)

# COMMAND ----------


