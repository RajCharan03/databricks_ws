# Databricks notebook source
text = ["my name is David and the dog name is Harley"]
sc = spark.sparkContext.parallelize(text)
sc.collect()

# COMMAND ----------

rdd1 = sc.flatMap(lambda word: word.split(' '))
rdd1.collect()

# COMMAND ----------

rdd2 =rdd1.map(lambda word:(word,1))
rdd2.collect()

# COMMAND ----------

from operator import add
rdd3 = rdd2.reduceByKey(add)
rdd3.collect()

# COMMAND ----------


