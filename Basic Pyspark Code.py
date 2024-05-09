# Databricks notebook source
# DBTITLE 1,Importing Functions
#import cell

from pyspark.sql.functions import col,column
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.sql.functions import expr, col, column

# COMMAND ----------

# DBTITLE 1,Reading a file
df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/athwanirahul@gmail.com/test.csv")
df1.show()

# COMMAND ----------

# DBTITLE 1,Defining Schema Saperately
orderSchema = 'Region String ,Country String ,ItemType String ,SalesChannel String ,OrderPriority String ,OrderID Integer ,UnitsSold Integer ,UnitPrice Double ,UnitCost Double ,TotalRevenue Double ,TotalCost Double ,TotalProfit Double'

df_order = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/athwanirahul@gmail.com/Order1_csv.csv", schema=orderSchema)


# COMMAND ----------

# DBTITLE 1,Actions
df_order.show()
df_order.count()
df_order.printSchema()
df_order.show(10)

# COMMAND ----------

display(df_order)

# COMMAND ----------

# DBTITLE 1,Checking Columns
df_order.columns

# COMMAND ----------

collectcolumnlist = df_order.columns

# COMMAND ----------

print(collectcolumnlist)
print(collectcolumnlist[3])

# COMMAND ----------

# DBTITLE 1,Creating Data Frame Manually
#creating manually dataframe

row1 = Row(1,'in-progress')
row2 = Row(2,'Completed')
row3 = Row(3,'not-started')

masterschema = 'id integer, status string'

# COMMAND ----------

#creating manually dataframe--different way

mandataframe = spark.createDataFrame([row1,row2,row3], masterschema)

# COMMAND ----------

mandataframe.show()

# COMMAND ----------

# DBTITLE 1,SQL in Data Frame
#Using SQL in dataframe--select

df_new = df_order.select("Region","Country")

# COMMAND ----------

df_new.show()

# COMMAND ----------

#Using SQL in dataframe--select--different way

df_new1 = df_order.select(col("Region"), column("Country"))
df_new1.show()

# COMMAND ----------

#Using SQL in dataframe--select--different way
df_order.show()
df_new2 = df_order.select(df_order.Region,df_order.Country)
df_order_part = df_order.select(col("Region"),col("Country"),col("ItemType"),col("UnitPrice"))
df_order_part.show()
df_order_part.write.format("csv").saveAsTable("rlapractice")

# COMMAND ----------

df_order.createOrReplaceTempView("orders")
df_order_part.createOrReplaceTempView("orders_new")
spark.sql("select * from orders_new")
df_sql1=spark.sql("select * from orders_new")
df_sql1.show()
df_sql1=spark.sql("select ItemType,sum(UnitPrice) from orders_new group by ItemType")
df_sql1.show()
df_sql1.count()


# COMMAND ----------

df_sql = spark.sql("select max(UnitsSold) as Maximum_units,region from orders group by Region")
df_sql.show()
What are the differences between saveAsTable and insertInto in different SaveMode(s)?



df_sql1.write.format("csv").saveAsTable("sidbhai.testing2")      




# COMMAND ----------

spark.sql("create database sidbhai")
