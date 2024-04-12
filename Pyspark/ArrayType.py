# Databricks notebook source
data = [('jack', [1,2]), 
        ('jhon',  [3,4]),
        ('david',  [5,6])
        ]


columns = ['id', 'name']

df = spark.createDataFrame(data, columns)
df.show()
df.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

data = [('jack', [1,2]), 
        ('jhon',  [3,4]),
        ('david',  [5,6])
        ]


schema = StructType([StructField(name = 'id', dataType = StringType()),\
                     StructField(name = 'number', dataType = ArrayType(IntegerType()))
                    ])

df = spark.createDataFrame(data, schema)
df.show()
df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

df1 = df.withColumn('num1', col('number')[0])
display(df1)

# COMMAND ----------

from pyspark.sql.functions import array

data = [(1,2),(3,4)]
schema = ['num1', 'num2']

df = spark.createDataFrame(data, schema)
df.display()

df1 = df.withColumn('number', array(df.num1, df.num2))
display(df1)
df1.printSchema()


# COMMAND ----------


