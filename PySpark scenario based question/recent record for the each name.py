# Databricks notebook source
data = [(1, 'Arul', 'Chennai', '2023-01-01'),
        (1, 'Arul', 'Bangalore', '2023-02-01'),
        (2, ' Sam', 'Chennai', '2023-01-01'),
        (3,'manish', 'patna', '2023-01-01'),
        (3, 'manish', 'patna', '2023-03-15'),
        (3, 'manish', 'patna','2023-02-27')]

# COMMAND ----------

schema = ['id', 'name', 'place', 'date']
df = spark.createDataFrame(data, schema)
df.show()

# COMMAND ----------

df.createOrReplaceTempView('newTable')

# COMMAND ----------

df1=spark.sql(" select * from (select id, name, place, date, rank() over(partition by id order by date desc) as rank from newTable) a where rank =1")
df1.show()

# COMMAND ----------


