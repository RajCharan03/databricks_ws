# Databricks notebook source
# MAGIC %scala
# MAGIC spark

# COMMAND ----------

data = [(1, 'Jack', 'M', 2000,'HR'),(2, 'Asi', 'F', 3000,'HR'),(3, 'Rock', 'M', 2000,'HR'),(4, 'Ayesha', 'F', 3000,'IT'),(5, 'Harley', 'M', 5000,'IT')]
schema = ['id', 'name', 'gender', 'salary','dep']

df = spark.createDataFrame(data, schema)
df.show()

# COMMAND ----------

df.createOrReplaceGlobalTempView('empGlobal')


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.empGlobal

# COMMAND ----------

spark.catalog.currentDatabase()

# COMMAND ----------

spark.catalog.listTables('default')

# COMMAND ----------

spark.catalog.listTables('global_temp')

# COMMAND ----------

spark.catalog.dropTempView('employess')

# COMMAND ----------


