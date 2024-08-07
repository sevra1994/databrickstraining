# Databricks notebook source
# MAGIC %fs ls
# MAGIC

# COMMAND ----------

""" Azure Path
dbutils.fs.mount(
  source = {wasbs://{Azure Path}.blob.core.windows.net},
  mount_point = {Folder Path},
  extra_configs = {Key path}:"{Access Key}")
  """

# COMMAND ----------

# MAGIC
# MAGIC %fs ls

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/ssdatabricksadls/

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/ssdatabricksadls/training/input_files/

# COMMAND ----------

# MAGIC %fs mk dir dbfs:/mnt/ssdatabricksadls/training/input_files/day2

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/mnt/ssdatabricksadls/training/input_files/day2")

# COMMAND ----------


