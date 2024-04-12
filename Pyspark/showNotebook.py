# Databricks notebook source
data = [(1, 'jasjaskabxalncvnasldvnsodnv'), 
        (2, 'lksnclancoralkjsclasc'),
        (3,'ashbckhabcksnascalscac'),
        (4,'yadbkanschajajajcajcajc')]

schema = ['id', 'comments']

df = spark.createDataFrame(data = data, schema = schema)
df.show() # it will only show 20 char and 20 rows
help(df.show)


# COMMAND ----------

df.show(truncate=False)

# COMMAND ----------

df.show(truncate=5)

# COMMAND ----------

df.show(n = 2, truncate=False)

# COMMAND ----------

df.show(n = 4, truncate=False, vertical=True)

# COMMAND ----------


