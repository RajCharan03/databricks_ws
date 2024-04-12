# Databricks notebook source
data = [(1, 'Arul', 'IT', '2000'),
        (2, ' Sam', 'IT', '4000'),
        (3,'manish', 'IT', '5000'),
        (4, 'Harley', 'HR', '6000'),
        (5, 'David', 'HR','6000')]

# COMMAND ----------

schema = ['id', 'name', 'dep', 'salary']
df = spark.createDataFrame(data, schema)
df.show()

# COMMAND ----------

df.createOrReplaceTempView('newTable')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from newTable where salary = (select max(salary) from newTable as a where a.salary <> (select max(b.salary) from newTable as b))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from newtable where dep in (select dep  from newTable group by dep having count(*) >2)

# COMMAND ----------


