# Databricks notebook source
# DBTITLE 1,Load Includes
# MAGIC %run "/Workspace/Users/sevra.shailesh456@gmail.com/includes"

# COMMAND ----------

# DBTITLE 1,Load File
df_races= spark.read.csv(f"{input_path}races.csv",header=True,inferSchema=True)

# COMMAND ----------

# DBTITLE 1,Transformation
df_races2=df_races.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("circuitId","circuit_id")\
.withColumn("ingestion_date",current_timestamp())\
.withColumn("path",input_file_name())\
.drop("url")

# COMMAND ----------

# DBTITLE 1,Load
df_races2.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.Races")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.Races

# COMMAND ----------


