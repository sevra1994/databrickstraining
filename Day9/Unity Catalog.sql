-- Databricks notebook source
use catalog hexaware

-- COMMAND ----------

create schema dev

-- COMMAND ----------

use dev;
create table employee(id int,name string)


-- COMMAND ----------

--grant usage on schema dev to ``

-- COMMAND ----------

insert into dev.employee values(1,'shailesh')

-- COMMAND ----------

insert into dev.employee values(2,'virat');
insert into dev.employee values(3,'rohit');
insert into dev.employee values(4,'jageja');

-- COMMAND ----------

update dev.employee set name='jadeja' where id=4;

-- COMMAND ----------

select * from dev.employee;

-- COMMAND ----------

create view dev.employee_id_count AS
  select COUNT(*) as count from dev.employee


-- COMMAND ----------

select * from dev.employee_id_count

-- COMMAND ----------


