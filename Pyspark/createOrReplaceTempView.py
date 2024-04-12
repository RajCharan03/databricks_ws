# Databricks notebook source
data = [(1, 'Jack', 'M', 2000,'HR'),(2, 'Asi', 'F', 3000,'HR'),(3, 'Rock', 'M', 2000,'HR'),(4, 'Ayesha', 'F', 3000,'IT'),(5, 'Harley', 'M', 5000,'IT')]
schema = ['id', 'name', 'gender', 'salary','dep']

df = spark.createDataFrame(data, schema)
df.show()

# COMMAND ----------

df.createOrReplaceTempView('employess')
spark.sql("select * from employess").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employess

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employess where gender = 'M'

# COMMAND ----------

# MAGIC %sql
# MAGIC select id, upper(name) as name from employess

# COMMAND ----------


