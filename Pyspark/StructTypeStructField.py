# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
data = [(1, ('Harley', 'david'), 3000),
        (2, ('John','cena'), 3000)]

structName = StructType([StructField(name = 'fristName', dataType = StringType()),\
                         StructField(name = 'secondName', dataType= StringType())\
                        ])

schema = StructType([StructField(name = 'id', dataType = IntegerType()),\
                     StructField(name = 'name', dataType = structName),\
                     StructField(name = 'salary', dataType = IntegerType())
                    ])

df = spark.createDataFrame(data, schema)
display(df)
df.printSchema()


# COMMAND ----------


