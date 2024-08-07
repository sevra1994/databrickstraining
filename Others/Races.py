# Databricks notebook source
Task:
    
Step1: Read the csv
 
Step 2: Transformation
- rename raceId to race_id and circuitId to circuit_id
- new column that should contain current timestamp
- new column that should contain path
- drop url column
 
step3: save it to table

# COMMAND ----------

df_races= spark.read.option("header",True).option("inferSchema",True).csv("dbfs:/mnt/ssdatabricksadls/raw/formula1/races.csv")

# COMMAND ----------

from pyspark.sql.functions import *

df_races2=df_races.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("circuitId","circuit_id")\
.withColumn("ingestion_date",current_timestamp())\
.withColumn("path",input_file_name())\
.drop("url")

# COMMAND ----------

df_races2.write.saveAsTable("formula1.Races")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.Races

# COMMAND ----------


