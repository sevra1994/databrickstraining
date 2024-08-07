# Databricks notebook source
employees = [(1, "Scott", "Tiger", 1000.0,
                      "united states"
                     )]
df = spark. \
    createDataFrame(employees,
                    schema="""employee_id INT, first_name STRING,
                    last_name STRING, salary FLOAT, nationality STRING
                    """
                   )
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table employee(
# MAGIC   employee_id int,
# MAGIC   first_name string,
# MAGIC   last_name string,
# MAGIC   salary int,
# MAGIC   nationality string
# MAGIC )

# COMMAND ----------

df.createOrReplaceTempView("source_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO employee as target
# MAGIC USING source_view as source
# MAGIC   on target.employee_id=source.employee_id
# MAGIC   WHEN MATCHED
# MAGIC THEN UPDATE SET
# MAGIC   target.first_name=source.first_name,
# MAGIC   target.last_name=source.last_name,
# MAGIC   target.salary=source.salary,
# MAGIC   target.nationality=source.nationality
# MAGIC   WHEN NOT MATCHED
# MAGIC THEN INSERT (employee_id,first_name,last_name,salary, nationality) values (employee_id,first_name,last_name,salary, nationality)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee

# COMMAND ----------

employees = [(1, "Scott", "Tiger", 1000.0,
                      "India" ),(2, "John", "Clair", 2000.0,
                      "United Kingdom"
                     )]
df = spark. \
    createDataFrame(employees,
                    schema="""employee_id INT, first_name STRING,
                    last_name STRING, salary FLOAT, nationality STRING
                    """
                   )
df.createOrReplaceTempView("source_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history employee

# COMMAND ----------


