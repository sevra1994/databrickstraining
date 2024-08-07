# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/ssdatabricksadls/raw/formula1/

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Querying the files
# MAGIC select * from json.`dbfs:/mnt/ssdatabricksadls/raw/formula1/drivers.json`

# COMMAND ----------

df_drivers= spark.read.json("dbfs:/mnt/ssdatabricksadls/raw/formula1/drivers.json")

# COMMAND ----------

##from pyspark.sql.functions import explode,col
#df = df.select('code','dob','driverId','driverRef','nationality','number','url', explode('name').alias('name_info'))
df_drivers.write.saveAsTable("formula1.drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.drivers

# COMMAND ----------

df_pit_stops= spark.read.option("multiline",True).json("dbfs:/mnt/ssdatabricksadls/raw/formula1/pit_stops.json")

# COMMAND ----------

df_pit_stops.show()

# COMMAND ----------

df_pit_stops.write.saveAsTable("formula1.pit_stops")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.pit_stops

# COMMAND ----------


