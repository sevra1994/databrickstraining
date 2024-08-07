# Databricks notebook source
# MAGIC %fs ls dbfs:/databricks-datasets/asa/airlines/

# COMMAND ----------

df_air=spark.read.csv("dbfs:/databricks-datasets/asa/airlines/",header=True,inferSchema=True)

# COMMAND ----------

# MAGIC %run "/Workspace/Users/sevra.shailesh456@gmail.com/Day5/includes"

# COMMAND ----------

df=spark.read.csv(f"{input}",header=True,inferSchema=True)

# COMMAND ----------

str_schema="name string,country string,industry string,net_worth_in_billion double,company string"

# COMMAND ----------

df=spark.read.schema(str_schema).csv(f"{input}",header=True,inferSchema=True)

# COMMAND ----------

df.display()

# COMMAND ----------

str_schema="name string,country string,industry string,net_worth_in_billion double,company string"

# COMMAND ----------

pyspark_schema=StructType([StructField("name",StringType()),
                           StructField("country",StringType()),
                           StructField("industry",StringType()),
                           StructField("net_worth",DoubleType()),
                           StructField("company",StringType())])

# COMMAND ----------

df_new2=spark.read.schema(pyspark_schema).csv(f"{input}",header=True,inferSchema=True)

# COMMAND ----------


