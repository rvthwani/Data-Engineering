# Databricks notebook source
#DBUtils fs help

dbutils.fs.help()

# COMMAND ----------

dbutils.notebook.help()


# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

#dbutils.fs.ls("/FileStore/tables/")
#dbutils.fs.head("/FileStore/tables/goalscorers-2.csv")
#dbutils.fs.mkdir("/FileStore/tables/Test")
#dbutils.fs.cp("from_path","To_Path")
#dbutils.fs.put("/FileStore/tables/Test.txt")
#dbutils.fs.mv()
#dbutils.fs.rm()

# COMMAND ----------

#Dbutils Notebook Commands

dbutils.notebook.run("notebookname",time of execution)

# COMMAND ----------

dbutils.widgets.text("Folder_name",",")
dbutils.widgets.text("File_name",",")
dbutils.widgets.remove("folder_name")

# COMMAND ----------

Folder_location=dbutils.widgets.get("Folder_name")
File_location=dbutils.widgets.get("File_name")

print("Folder variable is ",Folder_location)
print("File variable is ",File_location)

# COMMAND ----------

#Reading a file

# df=spark.read.format("csv")
# .option("infer schema", True)
# .option("header",True)
# .option("sep", ",")
# .load(Folder_name+File_name)

# COMMAND ----------

# dbutils.widgets.multiselect("Product","Camera",("Camera","GPS","Smartphone"))

# COMMAND ----------

#Explode with Array

# array_appliance = [
# ('Raja',['TV','Refrigerator','Oven','AC']),
# ('Raghav',['AC','Washing Machine',None]),
# ('Ram',['Grinder','TV']),
# ('Ramesh',['Refrigerator','TV',None]),
# ('Rajesh',None)
# ]

# df_app =  spark.createDataFrame(data=array_appliance,schema=['name','Appliances'])

# df_app.printSchema()
# display(df_app)



# COMMAND ----------

#Explode with Map


# map_brand = [

# ('Raja',{'TV':'LG','Refrigerator':'Samsung','Oven':'Philips','AC':'Voltas'}),
# ('Raghav',{'AC':'Samsung','Washing Machine':'LG'}),
# ('Ram',{'Grinder':'Preethi','TV':''}),
# ('Ramesh',{'Refrigerator':'LG','TV':'Croma'}),
# ('Rajesh',None)
# ]

#schema_data=['name','Brand']

df_brand = spark.createDataFrame(data=map_brand,schema=schema_data)
df_brand.printSchema()
display(df_brand)

# COMMAND ----------

# from pyspark.sql.functions import explode

# df2 = df_app.select(df_app.name,explode(df_app.Appliances))

# df_app.printSchema()
# display(df_app)

# df2.printSchema()
# display(df2)

# COMMAND ----------

#Explode -- It do not contain rows with NULL


# from pyspark.sql.functions import explode

# df3 = df_brand.select(df_brand.name,explode(df_brand.Brand))

# df_brand.printSchema()
# display(df_brand)

# df3.printSchema
# display(df3)

# COMMAND ----------

# #Explode Outer -- it contain rows with NULL as well


# from pyspark.sql.functions import explode_outer

# display(df_app.select(df_app.name, explode_outer(df_app.Appliances)))

# display(df_brand.select(df_brand.name,explode_outer(df_brand.Brand)))

# COMMAND ----------

# #Positional Explode -- It contain position column as well but avoid NULL valued Column

# from pyspark.sql.functions import posexplode

# display(df_app.select(df_app.name,posexplode(df_app.Appliances)))

# display(df_brand.select(df_brand.name,posexplode(df_brand.Brand)))

# COMMAND ----------

# #Positional outer Explode -- It contain position column as well but carry NULL valued Column

# from pyspark.sql.functions import posexplode_outer

# display(df_app.select(df_app.name,posexplode_outer(df_app.Appliances)))

# display(df_brand.select(df_brand.name,posexplode_outer(df_brand.Brand)))

# COMMAND ----------

#-------------------------------Case Function---------------------------------------------------------

Schema_student = ["Name","Subject","Marks","Status","Attendance"]

data_student = [
("Raja","Science",80,"P",90),
("Rakesh","Maths",90,"P",70),
("Rama","English",20,"F",80),
("Ramesh","Science",45,"F",75),
("Rajesh","Maths",30,"F",50),
("Raghav","Maths",None,"NA",70)
]

df = spark.createDataFrame(data=data_student,schema=Schema_student)
display(df)

# COMMAND ----------

from pyspark.sql.functions import when

df1 = df.withColumn("Status", when(df.Marks >= 50,"Pass").when(df.Marks <50,"Fail").otherwise("Absentee"))

display(df1)

# COMMAND ----------

from pyspark.sql.functions import when

df2 = df.withColumn("New_Status", when(df.Marks>=50,"Pass")
                    .when(df.Marks<50,"Fail")
                    .otherwise("Absentee"))


display(df2)

# COMMAND ----------

#-------------------------------------------Another Method--------------------------------------------------

from pyspark.sql.functions import expr

df3 = df.withColumn("new_status", expr("CASE WHEN Marks>=50 THEN 'Pass' " + 
                                       "WHEN Marks < 50 THEN 'Fail' " +
                                       "ELSE 'Absentee' END"))

display(df3)

# COMMAND ----------

#---------------------------------------Multi Conditions using AND and OR---------------------------------------------

from pyspark.sql.functions import when

df4 = df.withColumn("Grade", when((df.Marks>=80) & (df.Attendance>=80), "Distinction")
                    .when((df.Marks>=50) & (df.Attendance>=50) , 'Good')
                    .otherwise("Average"))
display(df4)                    

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").getOrCreate()

print(spark.sparkContext.version)

# COMMAND ----------


from pyspark.sql.functions import when


# employee_schema=["employee_id","name","doj","employee_dept_id","gender","salary"]
# employee_data=[(100,"Stephen","1999","100","M",2000),
#                (200,"Philip","2002","200","M",8000),
#                (300,"John","2010","100","",6000),
#                ]



# df1= spark.createDataFrame(data=employee_data,schema=employee_schema)

# display(df1)

df1= df1.withColumn("gender",when(df1.gender=='F','M')
                    .when(df1.gender=='M','M')
                    .otherwise (""))

display(df1)

# COMMAND ----------

employee_schema=["employee_id","name","doj","employee_dept_id","gender","salary"]
employee_data=[(100,"Stephen","1999","100","M",2000),
               (200,"Philip","2002","200","M",8000),
               (300,"John","2010","100","",6000),
               ]



df1= spark.createDataFrame(data=employee_data,schema=employee_schema)

display(df1)

# COMMAND ----------

employee_schema=["employee_id","name","doj","employee_dept_id","gender","salary"]
employee_data=[ (300,"John","2010","100","",6000),
               (400,"Nancy","2008","400","F",1000),
               (500,"Rosy","2014","500","M",5000),
               ]

df2 = spark.createDataFrame(data=employee_data,schema=employee_schema)

display(df2)

# COMMAND ----------

#--------------Union---------------------------------

df_union = df1.union(df2)
display(df_union) # union contain duplicate also

# COMMAND ----------

df3 = df_union.dropDuplicates()
display(df3)

# COMMAND ----------

#----------------------UnionAll-------------------------------

df_unionAll = df1.unionAll(df2)

display(df_unionAll)

# COMMAND ----------

df4 = df2.select(df2.employee_id,df2.name,df2.doj,df2.employee_dept_id,df2.gender)
display(df4)

# COMMAND ----------

df_invalie = df1.union(df4)

# COMMAND ----------

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

# COMMAND ----------

df = spark.read.format("csv")
.option("mode","FAILFAST")
.option("header","true")
.schema(schema)
.load("dbfs:/FileStore/shared_uploads/athwanirahul@gmail.com/Production_data_corrupt.csv")

display(df)

# COMMAND ----------

#---------Connecting ADLS-----------------------------------

spark.conf.set(
"fs.azure.account.key.keshavstorage.dfs.core.windows.net",
"Cyp1iTuvZWf7MTlsb9uOGd4jV6SG14WEkc7X2PQM2EmtH1C4mWJGLiP+fYAEqEic+s+byLGhZG0I+ASt6FHU1w==")



# COMMAND ----------

#--------------------Set the data lake file location--------------

file_location = "abfss://ksinha@keshavstorage.dfs.core.windows.net/"

#---------Read in the data to dataframedf----------

df = spark.read.format("csv").option("inferSchema","true").option("header","true").option("delimiter",",").load(file_location)

display(df)


# COMMAND ----------

dbutils.fs.ls("abfss://ksinha@keshavstorage.dfs.core.windows.net/")

# COMMAND ----------

#------------------Creating Mount Point Using ADLS--------------------

dbutils.fs.mount(
    source = "wasbs://ksinha@keshavstorage.blob.core.windows.net",
    mount_point = "/mnt/adls_test",
    extra_configs = {"fs.azure.account.key.keshavstorage.blob.core.windows.net":"Cyp1iTuvZWf7MTlsb9uOGd4jV6SG14WEkc7X2PQM2EmtH1C4mWJGLiP+fYAEqEic+s+byLGhZG0I+ASt6FHU1w=="}
    )

# COMMAND ----------

dbutils.fs.ls("/mnt/adls_test")

# COMMAND ----------

#--------To check how many mont points are created, we can give command...In real project multiple mont points are created to connect with storage accounts---------------

dbutils.fs.mounts()

#-----------Unmount a mount point-------------------

#dbutils.fs.unmount("/mnt/adls_test")

# COMMAND ----------

jdbcHostname = "keshav.database.windows.net"
jdbcPort = 1433
jdbcDatabase = "keshavdb"
jdbcUsername = "ksinha"
jdbcPassword = "Aradhya@123"
jdbcDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"


jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};databaseName={jdbcDatabase};user={jdbcUsername};password={jdbcPassword}"


# COMMAND ----------

df1=spark.read.format("jdbc").option("url",jdbcUrl).option("dbtable","dbo.employee").load()
display(df1)

# COMMAND ----------

#df2=spark.read.format("jdbc").option("url",jdbcUrl).option("dbtable","dbo.salary").load()
display(df2)

# COMMAND ----------

# #---------------using connection string------------------------

connectionstring="jdbc:sqlserver://keshav.database.windows.net:1433;database=keshavdb;user=ksinha@keshav;password={Aradhya@123};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

df=spark.read.jdbc(connectionstring,"dbo.Tax")

display(df)

# COMMAND ----------

#------------------------Transformation replacing Null Values and Duplicate values---------------------------------------
