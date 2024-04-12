# Databricks notebook source
data = [(1, "John", 30, "Sales", 50000.0),
(2, "Alice", 28, "Marketing", 60000.0),
(3, "Bob", 32, "Finance", 55000.0),
(4, "Sarah", 29, "Sales", 52000.0),
(5, "Mike", 31, "Finance", 58000.0)
]

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
schema = StructType([
                    StructField("id", IntegerType(), nullable=False),
                    StructField("name", StringType(), nullable=False),
                    StructField("age", IntegerType(), nullable=False),
                    StructField("department", StringType(), nullable=False),
                    StructField("salary", DoubleType(), nullable=False)
                    ])

# COMMAND ----------

df = spark.createDataFrame(data, schema)

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Question 1:Calculate the average salary for each department:

# COMMAND ----------

df1 = df.groupBy('department').agg(avg('salary').alias('avg_salary'))
df1.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Question 2:Add a new column named "bonus" that is 10% of the salary for all employees.

# COMMAND ----------

df.withColumn('bonus', col('salary')*0.1).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Question 3: Group the data by department and find the employee with the highest salary in each department

# COMMAND ----------

from pyspark.sql.window import *
windowFun = Window.partitionBy('department').orderBy(col('salary').desc())
df3 = df.withColumn('row_num', row_number().over(windowFun))
df4 = df3.filter(df3.row_num ==1)
df4.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ####Question 4: Find the top 2 departments with the highest total salary.

# COMMAND ----------

df.groupBy('department').agg(sum('salary').alias('total_salary')).orderBy(col('total_salary').desc()).limit(2).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Question5: Find the top most department having highest salary

# COMMAND ----------

df.groupBy('department').agg(sum('salary').alias('total_salary')).orderBy(desc('total_salary')).limit(1).select('department').show()

# COMMAND ----------

###using windows function 

df2 = df.groupBy('department').agg(sum('salary').alias('total_salary'))
windspc = Window.orderBy(desc(df2.total_salary))

df3 = df2.withColumn('row_num', row_number().over(windspc))
df4 = df3.filter(df3.row_num == 1).select('department')

df4.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Question 6:Filter the DataFrame to keep only employees aged 30 or above and working in the "Sales" department

# COMMAND ----------

df.filter((df.department == 'Sales') & (df.age >= 30)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Question 7:Calculate the difference between each employee's salary and the average salary of their respective department

# COMMAND ----------

windfun = Window.partitionBy('department')
df1 = df.withColumn('avg_salary', avg('salary').over(windfun))
df2 = df1.withColumn('salary_diff', col('salary') - col('avg_salary'))
df2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####8. Calculate the sum of salaries for employees whose names start with the letter "J".

# COMMAND ----------

df1 = df.where(df.name.like('J%') ).agg(sum('salary').alias('total_salary'))
df1.show()

# COMMAND ----------

df1 = df.filter(col('name').startswith('J') ).agg(sum('salary').alias('total_salary'))
df1.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####9. Sort the DataFrame based on the "age" column in ascending order and then by "salary" column in descending order

# COMMAND ----------

df.sort(df.age.asc(), df.salary.desc()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####10. Replace the department name "Finance" with "Financial Services" in the DataFrame:

# COMMAND ----------

df.withColumn('department', when(col('department')=='Finance', 'Financial Services').otherwise(col('department'))).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####11. Calculate the percentage of total salary each employee contributes to their respective department.

# COMMAND ----------

winfu = Window.partitionBy('department')
df1 = df.withColumn('avg_sal', avg('salary').over(winfu))
df2 = df1.withColumn('total_sal', sum('salary').over(winfu))

percentage_sal = (col('salary') / col('total_sal')) * 100

df3 = df2.withColumn('perc_salaray', round(percentage_sal,2))
df3.sort(col('perc_salaray').asc()).show()

# COMMAND ----------


