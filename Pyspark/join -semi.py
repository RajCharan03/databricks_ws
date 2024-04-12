# Databricks notebook source
data1 = [(1, 'Jack', 2000,2),(2, 'Asi', 2000,1),(3, 'Harley', 2000,4)]
schema1 = ['id', 'name', 'salary','dep']

data2 = [(1, 'IT'),(2,'HR'),(3,'Payroll')]
schema2 = ['id', 'name']

empDF = spark.createDataFrame(data1, schema1)
depDF = spark.createDataFrame(data2, schema2)
empDF.show()
depDF.show()

# COMMAND ----------

empDF.join(depDF, empDF.dep==depDF.id, 'inner').show()
empDF.join(depDF, empDF.dep==depDF.id, 'leftsemi').show()

# COMMAND ----------

empDF.join(depDF, empDF.dep==depDF.id, 'leftanti').show()

# COMMAND ----------

data = [(1, 'Jack',0),(2, 'Asi', 1),(3, 'Harley', 2)]
schema = ['id', 'name', 'managerId']

dfemp = spark.createDataFrame(data, schema)

from pyspark.sql.functions import col
dfemp.alias('empData').join(dfemp.alias('mgrData'),\
                        col('empData.managerId') == col('mgrData.id') ,'left')\
                        .select(col('empData.name').alias('empName'), col('mgrData.name').alias('mgeName')).show()

# COMMAND ----------


