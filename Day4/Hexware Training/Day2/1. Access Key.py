# Databricks notebook source
# MAGIC %fs ls

# COMMAND ----------

""" Azure Path
dbutils.fs.mount(
  source = {wasbs://{Path}.blob.core.windows.net},
  mount_point = "/mnt/datamasterdatabricks/raw",
  extra_configs = {{Path}.blob.core.windows.net":"{Access Key}"})
"""
