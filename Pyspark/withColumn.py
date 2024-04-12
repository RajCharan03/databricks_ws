# Databricks notebook source
data = [(1,'jack','3000'), 
        (2,'jhon', '3000')]


columns = ['id', 'name','salary']

df = spark.createDataFrame(data = data, schema = columns)

df.show()
df.printSchema()

# COMMAND ----------

help(df.withColumn)

# COMMAND ----------

from pyspark.sql.functions import col,lit
df1 = df.withColumn(colName='salary', col=col('salary').cast('Integer'))

df1.show()
df1.printSchema()

# COMMAND ----------

df2 = df1.withColumn('salary', col('salary')*2)
df2.show()

# COMMAND ----------

df3 = df2.withColumn('country',lit('India') )
df3.show()

# COMMAND ----------

display(df3.withColumn('copiedSalary', col('salary')))

# COMMAND ----------


