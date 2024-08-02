# Databricks notebook source
# MAGIC %fs ls

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/ssdatabricksadls/raw/formula1/
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Querying the files
# MAGIC select * from json.`dbfs:/mnt/ssdatabricksadls/raw/formula1/constructors.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema formula1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create table
# MAGIC create table formula1.constructors as 
# MAGIC select * from json.`dbfs:/mnt/ssdatabricksadls/raw/formula1/constructors.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1.constructors

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Querying the files
# MAGIC select * from csv.`dbfs:/mnt/ssdatabricksadls/raw/formula1/circuits.csv`
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Querying the files
# MAGIC create table formula1.circuits as 
# MAGIC select * from csv.`dbfs:/mnt/ssdatabricksadls/raw/formula1/circuits.csv`

# COMMAND ----------

# MAGIC %sql
# MAGIC create table (circuitId int,circuitRef string,name string,location string,country string,lat string,lng	alt	url)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1.circuits

# COMMAND ----------

df= spark.read.option("header",True).option("inferSchema",True).csv("dbfs:/mnt/ssdatabricksadls/raw/formula1/circuits.csv")

# COMMAND ----------

df.write.saveAsTable("formula1.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.circuits

# COMMAND ----------


