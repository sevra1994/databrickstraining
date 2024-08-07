# Databricks notebook source
schema=["name", "subject", "Marks", "Attendance"]
 
student_data=[("John","Math", 90, 80),("Michael", "Science", 70, None), ("David", "History", 50,40), ("Kelvin", "Computer", 40,None ),("Paul", "GEO", None, None), (None,None,  None, None),("John","Math", 90, 80),("John","Math", 90, 80),(None,None,  None, None),(None,None,  None, None),(None,None,  None, None),("Michael", "Science", 70, None),(None, "Science", 90, None),(" ", "NAN", 55, None)]
 
df=spark.createDataFrame(data=student_data, schema=schema)
display(df)
df.printSchema()
 

# COMMAND ----------

df.distinct().display()

# COMMAND ----------

df1=df.dropDuplicates()

# COMMAND ----------

help(df.dropna)

# COMMAND ----------

df1.display()

# COMMAND ----------

df1.dropna("all").display()

# COMMAND ----------

df2=df1.dropna(subset="name")

# COMMAND ----------

df3=df2.fillna({"Marks":39,"Attendance":34})

# COMMAND ----------

df3.display()

# COMMAND ----------

df3.replace("NAN","IT",subset="subject").replace(" ","Robert",subset="name").display()

# COMMAND ----------


