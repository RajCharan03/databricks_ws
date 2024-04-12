# Databricks notebook source
df = spark.range(start=1, end=101)
display(df)

# COMMAND ----------

df.sample(fraction=0.1).show()

# COMMAND ----------

display(df.sample(fraction=0.1, seed = 123))

# COMMAND ----------

display(df.sample(fraction=0.1, seed = 123))

# COMMAND ----------


