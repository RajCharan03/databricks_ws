# Databricks notebook source
from pyspark.sql import Row

row = Row('David', 2000)
print(row[0]+' '+str(row[1]))

# COMMAND ----------

row = Row(name = 'David', rating = 2000)
print(row.name+' '+str(row.rating))

# COMMAND ----------

row1 = Row(name = 'David', rating = 2000)
row2 = Row(name = 'Harley', rating = 3000)

data = [row1, row2]
df = spark.createDataFrame(data)
df.show()
df.printSchema()

# COMMAND ----------

person = Row('name', 'rating')
p1 = person('HDavid', 2000)
p2 = person('HarleyD', 3000)

print(p1.name)
data1 = [p1,p2]
df1 = spark.createDataFrame(data1)
df1.show()
df1.printSchema()


# COMMAND ----------


from pyspark.sql.functions import explode,col
data = [Row(name = 'David', prop = Row(age = 26, gender = 'male')),\
        Row(name = 'Harley', prop = Row(age = 25, gender = 'male'))]

df = spark.createDataFrame(data)
df.show()


# COMMAND ----------


