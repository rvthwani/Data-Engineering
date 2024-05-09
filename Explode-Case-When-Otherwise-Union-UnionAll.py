# Databricks notebook source
# DBTITLE 1,Reading a file
#Reading a file

# df=spark.read.format("csv")
# .option("infer schema", True)
# .option("header",True)
# .option("sep", ",")
# .load(Folder_name+File_name)

# COMMAND ----------

# DBTITLE 1,Explode
#Explode -- It do not contain rows with NULL


# from pyspark.sql.functions import explode

# df3 = df_brand.select(df_brand.name,explode(df_brand.Brand))

# df_brand.printSchema()
# display(df_brand)

# df3.printSchema
# display(df3)

# COMMAND ----------

# DBTITLE 1,Explode with Array
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

# DBTITLE 1,Explode with Map
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

# DBTITLE 1,Explode - Outer
# #Explode Outer -- it contain rows with NULL as well


# from pyspark.sql.functions import explode_outer

# display(df_app.select(df_app.name, explode_outer(df_app.Appliances)))

# display(df_brand.select(df_brand.name,explode_outer(df_brand.Brand)))

# COMMAND ----------

# DBTITLE 1,Positional Explode
# #Positional Explode -- It contain position column as well but avoid NULL valued Column

# from pyspark.sql.functions import posexplode

# display(df_app.select(df_app.name,posexplode(df_app.Appliances)))

# display(df_brand.select(df_brand.name,posexplode(df_brand.Brand)))

# COMMAND ----------

# DBTITLE 1,Positional outer Explode
# #Positional outer Explode -- It contain position column as well but carry NULL valued Column

# from pyspark.sql.functions import posexplode_outer

# display(df_app.select(df_app.name,posexplode_outer(df_app.Appliances)))

# display(df_brand.select(df_brand.name,posexplode_outer(df_brand.Brand)))

# COMMAND ----------

# DBTITLE 1,Case Function
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

# DBTITLE 1,When - Otherwise
from pyspark.sql.functions import when

df2 = df.withColumn("New_Status", when(df.Marks>=50,"Pass")
                    .when(df.Marks<50,"Fail")
                    .otherwise("Absentee"))


display(df2)

# COMMAND ----------

# DBTITLE 1,Method - 2
#-------------------------------------------Another Method--------------------------------------------------

from pyspark.sql.functions import expr

df3 = df.withColumn("new_status", expr("CASE WHEN Marks>=50 THEN 'Pass' " + 
                                       "WHEN Marks < 50 THEN 'Fail' " +
                                       "ELSE 'Absentee' END"))

display(df3)

# COMMAND ----------

# DBTITLE 1,Multi Conditions using AND and OR
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

# DBTITLE 1,Union
#--------------Union---------------------------------

df_union = df1.union(df2)
display(df_union) # union contain duplicate also

# COMMAND ----------

# DBTITLE 1,Drop Duplicates
df3 = df_union.dropDuplicates()
display(df3)

# COMMAND ----------

# DBTITLE 1,UnionAll
#----------------------UnionAll-------------------------------

df_unionAll = df1.unionAll(df2)

display(df_unionAll)

# COMMAND ----------


