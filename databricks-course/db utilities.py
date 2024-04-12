# Databricks notebook source
# MAGIC %fs
# MAGIC ls /databricks-datasets/

# COMMAND ----------

dbutils.fs.ls('/databricks-datasets/COVID/')

# COMMAND ----------

for files in dbutils.fs.ls('/databricks-datasets/COVID/'):
    if files.name.endswith('/'):
        print(files)

    

# COMMAND ----------

for files in dbutils.fs.ls('/databricks-datasets/COVID/'):
    if files.size == (6148):
        print(files.name)

# COMMAND ----------

dbutils.help()

# COMMAND ----------


