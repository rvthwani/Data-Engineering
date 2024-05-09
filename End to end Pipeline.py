# Databricks notebook source
# DBTITLE 1,Step 1 :- Extract data from Azure SQL 
jdbcHostname = "ke****.database.windows.net"
jdbcPort = 1433
jdbcDatabase = "k*****"
jdbcUsername = "k****"
jdbcPassword = "*******"
jdbcDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"


jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};databaseName={jdbcDatabase};user={jdbcUsername};password={jdbcPassword}"

# COMMAND ----------

# DBTITLE 1,Read Employee table
df1=spark.read.format("jdbc").option("url",jdbcUrl).option("dbtable","dbo.employee").load()
display(df1)

# COMMAND ----------

# DBTITLE 1,Read Salary table
df3=spark.read.format("jdbc").option("url",jdbcUrl).option("dbtable","dbo.salary").load()
display(df3)

# COMMAND ----------

# DBTITLE 1,Step-2:- Transformation replacing null value and dropping duplicate columns
df_employee_cleansed = df1.na.fill({"gender":"F"})
display(df_employee_cleansed)


df_salary_cleansed = df3.dropDuplicates()
display(df_salary_cleansed)

# COMMAND ----------

# DBTITLE 1,Joining the data
df_join = df_employee_cleansed.join(df_salary_cleansed,df_employee_cleansed.id==df_salary_cleansed.id,"inner").select(df_employee_cleansed.id,df_employee_cleansed.Name,
df_employee_cleansed.gender,
df_salary_cleansed.salary)
display(df_join)

# COMMAND ----------

# DBTITLE 1,Connecting to ADLS:- creating Mount points
dbutils.fs.mount(
    source = "wasbs://ksinha@keshavstorage.blob.core.windows.net",
    mount_point = "/mnt/adls_test",
    extra_configs = {"fs.azure.account.key.keshavstorage.blob.core.windows.net":"Cyp1iTuvZWf7MTlsb9uOGd4jV6SG14WEkc7X2PQM2EmtH1C4mWJGLiP+fYAEqEic+s+byLGhZG0I+ASt6FHU1w=="}
    )

# COMMAND ----------

dbutils.fs.ls("/mnt/adls_test")

# COMMAND ----------

# DBTITLE 1,Write data into Azure Data lake
df_join.write.format("csv").save("/mnt/adls_test/adv_work_csv/")
