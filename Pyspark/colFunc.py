# Databricks notebook source
data = [(1, 'Jack', 'M', 2000),(2, 'Asi', 'F', 2000), (3,'David','M', 2000)]
schema = ['id', 'name', 'Gender', 'salary']

df = spark.createDataFrame(data, schema)
df.select(df.id.alias('emp_id'),df.name.alias('emp_name'),df.Gender.alias('emp_gen')).show()

# COMMAND ----------

df.sort(df.name.desc()).show()

# COMMAND ----------

df.sort(df.id.asc()).show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df1 = df.select(df.id,df.name,df.salary.cast('int'))
df1.printSchema()

# COMMAND ----------

df1.filter(df.name.like('D%')).show()

# COMMAND ----------


