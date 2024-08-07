# Databricks notebook source
df_circuits= spark.read.option("header",True).option("inferSchema",True).csv("dbfs:/mnt/ssdatabricksadls/raw/formula1/circuits.csv")

# COMMAND ----------

from pyspark.sql.functions import *

df_circuits.select(col("circuitId"),"name",df_circuits["location"]).display()

# COMMAND ----------

df_circuits.select(concat("location",lit(" "),"country")).display()

# COMMAND ----------

df_circuits.withColumnRenamed("circuitId","circuit_id").display()

# COMMAND ----------

df_circuits.withColumn("ingestion_date",current_date()).display()

# COMMAND ----------

(df_circuits.withColumn("ingestion_date",current_date())
.withColumn("path",input_file_name())
.display())

# COMMAND ----------


