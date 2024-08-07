# Databricks notebook source
# MAGIC %sql
# MAGIC create table demo(id int, name string)

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended demo

# COMMAND ----------

# MAGIC %sql
# MAGIC Insert into demo values (1,'shailesh')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema delta

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/ssdatabricksadls/

# COMMAND ----------

# MAGIC %sql
# MAGIC create table delta.demo (id int,name string,age int) LOCATION 'abfss://raw@ssdatabricksadls.dfs.core.windows.net/delta/demo'

# COMMAND ----------

index for delta table using z order , liqued clustering for indexing for table and increase the performance

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into delta.demo values(1,'dhoni',36)

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into delta.demo values(1,'virat',30)

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into delta.demo values(1,'rohit',30);

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from delta.demo where name='rohit'

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from delta.demo where name='dhoni'

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into delta.demo values (2,'rohit',31)

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into delta.demo values (3,'jaddu',31);
# MAGIC insert into delta.demo values (4,'rahul',34);
# MAGIC insert into delta.demo values (5,'shubham',25);
# MAGIC insert into delta.demo values (6,'sai',25);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.demo

# COMMAND ----------

# MAGIC %sql
# MAGIC update delta.demo set name='Jadeja' where id=3

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.demo

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history delta.demo

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended delta.demo

# COMMAND ----------

# MAGIC %sql
# MAGIC --restore table table name version as of version number
# MAGIC --vacuum table name (delete unused table)
# MAGIC -- Retention period is 7 days for remove file using vacuum
# MAGIC -- imideate impact used vacuum table name retain 0 hours
