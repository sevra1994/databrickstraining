-- Databricks notebook source
CREATE STREAMING TABLE sales_bronze
comment "the raw sales table"
as 
SELECT *,current_timestamp() AS creation_date FROM cloud_files("dbfs:/mnt/hexawaredatabricks/raw/dlt_input/sales/","csv",map("cloudFiles.inferColumnTypes","true") )

-- COMMAND ----------

CREATE STREAMING TABLE customers_bronze
comment "the raw customers table"
as 
SELECT *,current_timestamp() AS creation_date FROM cloud_files("dbfs:/mnt/hexawaredatabricks/raw/dlt_input/customers/","csv",map("cloudFiles.inferColumnTypes","true") )

-- COMMAND ----------

CREATE STREAMING TABLE product_bronze
comment "the raw product table"
as 
SELECT *,current_timestamp() AS creation_date FROM cloud_files("dbfs:/mnt/hexawaredatabricks/raw/dlt_input/product/","csv",map("cloudFiles.inferColumnTypes","true") )

-- COMMAND ----------

CREATE STREAMING LIVE TABLE sales_silver
(CONSTRAINT order_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW)
as
select distinct(*) from STREAM(LIVE.sales_bronze)

-- COMMAND ----------


