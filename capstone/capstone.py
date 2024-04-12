# Databricks notebook source
client_secret = dbutils.secrets.get(scope="cap_secret",key="client-secret")
directory_id =  dbutils.secrets.get(scope="cap_secret",key="directory-id")
client_id =  dbutils.secrets.get(scope="cap_secret",key="client-id")
storage_account_name = "ucdpstorage1"
container_name = "raw"
mount_point = "files"

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": f"{client_id}",
          "fs.azure.account.oauth2.client.secret": f"{client_secret}",
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{directory_id}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.


# COMMAND ----------

dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{mount_point}",
  extra_configs = configs)

# COMMAND ----------

df = spark.read.csv('dbfs:/mnt/files/circuits1.csv', header=True)
df.show()

# COMMAND ----------

# MAGIC %fs
# MAGIC mounts

# COMMAND ----------


