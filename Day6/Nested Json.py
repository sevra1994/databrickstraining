# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/hexawaredatabricks/raw/input_files/adobe_sample.json

# COMMAND ----------

# MAGIC %run "/Workspace/Users/sevra.shailesh456@gmail.com/Day5/includes"

# COMMAND ----------

df=spark.read.json("dbfs:/mnt/hexawaredatabricks/raw/input_files/adobe_sample.json",multiLine=True)

# COMMAND ----------

df.display()

# COMMAND ----------

df1=df.withColumn("batters",explode("batters.batter")).withColumn("batters_id",col("batters.id")).withColumn("batters_type",col("batters.type")).withColumn("topping",explode("topping")).withColumn("topping_id",col("topping.id")).withColumn("topping_type",col("topping.type")).drop("topping").drop("batters")

# COMMAND ----------

df1.createOrReplaceTempView("adobe")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from adobe;

# COMMAND ----------

df1.filter("batters_type='Chocolate'").display()

# COMMAND ----------

df1.sort(col("topping_id").desc(),"batters_type").display()

# COMMAND ----------

# MAGIC %md
# MAGIC default partition for wide transformation is 200 in wide transormation
# MAGIC

# COMMAND ----------

df1.where("batters_type='Chocolate'").explain()

# COMMAND ----------


