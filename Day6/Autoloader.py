# Databricks notebook source
(spark
.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.inferColumnTypes",True)
.option("cloudFiles.schemaLocation","dbfs:/mnt/hexawaredatabricks/raw/schemalocation/shailesh/autoloader")
.load("dbfs:/mnt/hexawaredatabricks/raw/stream_in/")
.writeStream
.option("checkpointLocation","dbfs:/mnt/hexawaredatabricks/raw/checkpoint/shailesh/autoloader")
.trigger(once=True)
.table("ssdatabricks.bronze.autoloader")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ssdatabricks.bronze.autoloader

# COMMAND ----------

# DBTITLE 1,Final
(spark
.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.inferColumnTypes",True)
.option("cloudFiles.schemaEvolutionMode","addNewColumns")
.option("cloudFiles.schemaLocation","dbfs:/mnt/hexawaredatabricks/raw/schemalocation/shailesh/autoloader")
.load("dbfs:/mnt/hexawaredatabricks/raw/stream_in/")
.writeStream
.option("checkpointLocation","dbfs:/mnt/hexawaredatabricks/raw/checkpoint/shailesh/autoloader")
.option("mergeSchema",True)
.table("ssdatabricks.bronze.autoloader")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ssdatabricks.bronze.autoloader

# COMMAND ----------


