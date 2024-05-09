# Databricks notebook source
# DBTITLE 1,Pivot and Unpivot
#----------------Pivot and UnPivot----------------------------------------------------

data = [("ABC","Q1",2000),
        ("ABC","Q2",3000),
        ("ABC","Q3",6000),
        ("ABC","Q4",1000),
        ("XYZ","Q1",5000),
        ("XYZ","Q2",4000),
        ("XYZ","Q3",1000),
        ("XYZ","Q4",2000),
        ("KLM","Q1",2000),
        ("KLM","Q2",3000),
        ("KLM","Q3",1000),
        ("KLM","Q4",5000)
        ]


columns = ["Company","Quarter","Revenue"]

df = spark.createDataFrame(data = data, schema = columns)

display(df)

# COMMAND ----------

df_pivot = df.groupBy("Company").pivot("Quarter").sum("Revenue")

display(df_pivot)

# COMMAND ----------

# DBTITLE 1,Unpivot
#------------------------------UnPivot---------------------------------------------

df_unpivot = df_pivot.selectExpr("Company", "stack(4,'Quarter1',Q1,'Quarter2',Q2,'Quarter3',Q3,'Quarter4',Q4) as (Quarter,Revenue)")

display(df_unpivot)

# COMMAND ----------

df = spark.read.format("csv").option("header","true").option("inferschema","true").load("dbfs:/FileStore/shared_uploads/athwanirahul@gmail.com/Production_data_corrupt.csv")

display(df)

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType

schema = StructType([ \
    StructField("Month",StringType(),True), \
    StructField("Emp_Count",IntegerType(),True),\
    StructField("Production_Unit",IntegerType(),True),\
    StructField("Expense",IntegerType(),True),\
    StructField("_corrupt_record",StringType(),True)
])                
