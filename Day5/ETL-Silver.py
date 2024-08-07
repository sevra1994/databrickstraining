# Databricks notebook source
# MAGIC %sql
# MAGIC create schema if not exists ssdatabricks.silver

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace table silver.richest_silver as
# MAGIC select name, country, industry, net_worth_in_billions, company from ssdatabricks.bronze.richest_person

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists ssdatabricks.gold;
# MAGIC use gold

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table gold.country_count as  
# MAGIC select country, count(country) as count from ssdatabricks.silver.richest_silver group by country order by count desc

# COMMAND ----------


