# Databricks notebook source
# MAGIC %run "/Workspace/Users/sevra.shailesh456@gmail.com/Day5/includes"

# COMMAND ----------

input

# COMMAND ----------

dbutils.widgets.text("enviroment","")
w=dbutils.widgets.get("enviroment")

# COMMAND ----------

df=spark.read.csv(f"{input}",header=True,inferSchema=True)

# COMMAND ----------

df1=add_ingestion(df)

# COMMAND ----------

df1.display()

# COMMAND ----------

df1.columns

# COMMAND ----------

new_col=['name', 'country', 'industry', 'net_worth_in_billions', 'company','ingestion_date']

# COMMAND ----------

df2=df1.toDF(*new_col)

# COMMAND ----------

df2.display()

# COMMAND ----------

df2.write.mode("overwrite").save(f"{output}shailesh/richest")

# COMMAND ----------

# MAGIC %fs ls "dbfs:/mnt/hexawaredatabricks/raw/output_files/"

# COMMAND ----------

#df1.createOrReplaceTempView("richest_view")
#df1.createOrReplaceGlobalTempView("richest_globalview")
#%sql
#select * from global_temp.richest_globalview

# COMMAND ----------

df3=df2.withColumn("enviroment",lit(w))

# COMMAND ----------

df3.write.mode("overwrite").option("mergeSchema","true").save(f"{output}shailesh/richest")

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog ssdatabricks;
# MAGIC create schema if not exists bronze;
# MAGIC use schema bronze

# COMMAND ----------

df3.write.mode("overwrite").option("mergeSchema","true").saveAsTable("richest_person")

# COMMAND ----------


