# Databricks notebook source
# DBTITLE 1,Create Sample DataFrame
Transaction = [
    (100,'Cosmetic',150),
    (200,'Apperal',250),
    (300,'Shirt',400),
    (400,'Trouser',500),
    (500,'Socks',20),
    (100,'Belt',70),
    (200,'Cosmetic',250),
    (300,'Shoe',400),
    (400,'Socks',25),
    (500,'Shorts',100)
]

transactionDF = spark.createDataFrame(data=Transaction, schema = ['Store_id','Item','Amount'])

transactionDF.show()

# COMMAND ----------

# DBTITLE 1,Create Sample Dimension Table
Store = [
    (100,'Store_London'),
    (200,'Store_Paris'),
    (300,'Store_Frankfurt'),
    (400,'Store_Stockholm'),
    (500,'Store_Oslo')
]

storeDF = spark.createDataFrame(data = Store, schema=['Store_id','Store_name'])

storeDF.show()

# COMMAND ----------

from pyspark.sql.functions import broadcast

joinDF= transactionDF.join(broadcast(storeDF),transactionDF['Store_id'] == storeDF['Store_id'])

# COMMAND ----------

joinDF.show() 

# COMMAND ----------

joinDF.explain()

# COMMAND ----------

joinDF.explain(True)

# COMMAND ----------

# DBTITLE 1,Create Sample DataFrame

Schema_student = ["Name","Subject","Marks","Status","Attendance"]

data_student = [
("Micheal","Science",80,"P",90),
("Nancy","Maths",90,"P",None),
("David","English",20,"F",80),
("John","Science",None,"F",None),
("Blessy",None,30,"F",50),
("Martin","Maths",None,None,70)
]

df = spark.createDataFrame(data=data_student,schema=Schema_student)
display(df)

# COMMAND ----------

# DBTITLE 1,Filter All records with Null Value
#display(df.filter(df.Marks.isNull()))

#display(df.filter("Marks IS NULL"))

from pyspark.sql.functions import col
display(df.filter(col("Marks").isNull()))

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Filter all records without Null 
# display(df.filter(df.Marks.isNotNull()))

# display(df.filter((df.Marks.isNotNull()) & (df.Attendance.isNotNull())))


display(df.filter((df.Marks.isNotNull()) | (df.Attendance.isNotNull())))

# COMMAND ----------

# DBTITLE 1,Sample DataFrame
Schema_student = ["Name","Subject","Marks","Status","Attendance"]

data_student = [
("Micheal","Science",80,"P",90),
("Nancy","Maths",90,"P",None),
("David","English",20,"F",80),
("John","Science",None,"F",None),
("Martin","Maths",None,None,70),
(None,None,None,None,None)
]

df = spark.createDataFrame(data=data_student,schema=Schema_student)
display(df)

# COMMAND ----------

# DBTITLE 1,Drop the records with Null Value - All & ANY
# display(df.na.drop())# will remove all the entries (rows & columns) with null value

# display(df.na.drop("any"))# will remove all the entries (rows & columns) with null value

# display(df.na.drop("all"))# will remove all the entries (rows & columns) with all null values

display(df.dropna("any"))

# COMMAND ----------

# display(df.na.drop(subset=["Marks"]))# single column value

display(df.na.drop(subset=["Marks","Attendance"]))# multiple column value

# COMMAND ----------

# DBTITLE 1,Fill values for all columns if Null is present
#display(df.na.fill(value=0))# only integer columns will get replaced

# display(df.na.fill(value="NA"))# only string columns will get replaced

#display(df.fillna(value="NA"))# only string columns will get replaced

# display(df.fillna(value=0))# only string columns will get replaced

#display(df.na.fill(value=0,subset=["Marks","Attendance"]))# only string columns will get replaced

display(df.na.fill({"Marks":0, "Status":"NA","Name":"No_Name", "Subject":"English","Attendance":50}))

# COMMAND ----------

employee_schema=["employee_id","name","doj","employee_dept_id","salary"]
employee_data=[(10,"Michael Robinson","1999-06-01","100",2000),
               (20,"James Wood","2003-03-01","200",8000),
               (30,"Chris Andrews","2005-04-01","100",6000),
               (40,"Mark Bond","2008-10-01","100",7000),
               (50,"Steve Watson","1996-02-01","400",1000),
               (60,"Mathews Simon","1998-11-01","500",5000),
               (70,"Peter Paul","2011-04-01","600",5000)
               ]



empDF= spark.createDataFrame(data=employee_data,schema=employee_schema)

display(empDF)

# COMMAND ----------

# DBTITLE 1,Define UDF to Rename Column
import pyspark.sql.functions as f

def rename_columns(rename_df):
    for column in rename_df.columns:
        new_column = "Col_" +column
        rename_df = rename_df.withColumnRenamed(column,new_column)
    return rename_df

# COMMAND ----------

# DBTITLE 1,Execute UDF
renamed_df = rename_columns(empDF)
display(renamed_df)

# COMMAND ----------

# DBTITLE 1,UDF to convert name into upper case
from pyspark.sql.functions import upper,col


def upperCase_col(df):

    empDF_upper = df.withColumn("name_upper",upper(df.name))

    return empDF_upper

# COMMAND ----------

Up_Case_DF = upperCase_col(empDF)
display(Up_Case_DF)

# COMMAND ----------

# DBTITLE 1,Split Function-- Create Sample Dataframe
employee_schema=["employee_id","name","doj","employee_dept_id","salary"]
employee_data=[(10,"Michael Robinson","1999-06-01","100",2000),
               (20,"James Wood","2003-03-01","200",8000),
               (30,"Chris Andrews","2005-04-01","100",6000),
               (40,"Mark Bond","2008-10-01","100",7000),
               (50,"Steve Watson","1996-02-01","400",1000),
               (60,"Mathews Simon","1998-11-01","500",5000),
               (70,"Peter Paul","2011-04-01","600",5000)
               ]



empDF= spark.createDataFrame(data=employee_data,schema=employee_schema)

display(empDF)

# COMMAND ----------

from pyspark.sql.functions import split,col


df1 = empDF.withColumn('First_name',split(empDF['name'], ' ').getItem(0))\
      .withColumn('Last_Name', split(empDF['name'],' ').getItem(1))

display(df1)

# COMMAND ----------

# DBTITLE 1,Method 2
import pyspark

split_col = pyspark.sql.functions.split(empDF['name'],' ')

df2 = empDF.withColumn('First_name', split_col.getItem(0))\
        .withColumn('Last_name', split_col.getItem(1))

display(df2)

# COMMAND ----------

# DBTITLE 1,Third Method
split_col = pyspark.sql.functions.split(empDF['doj'], '-')

df3 = empDF.select("employee_id","name","employee_dept_id","salary",
                   split_col.getItem(0).alias('joining_year'),split_col.getItem(1).alias('joining_month'), split_col.getItem(2).alias('joining_day'))

display(df3)

# COMMAND ----------

# DBTITLE 1,Create Sample Data Frame
array_data = [
    ("John",4 , 1),
    ("John",6 , 2),
    ("David",7 , 3),
    ("Mike",3 , 4),
    ("David",5 , 2),
    ("John", 7 , 3),
    ("John", 9 , 7),
    ("David",1 , 8),
    ("David",4 , 9),
    ("David",7 , 4),
    ("Mike",8 , 5),
    ("Mike",5 , 2),
    ("Mike",3 , 8),
    ("John",2 , 7),
    ("David",1 , 9)
]

array_schema = ["Name", "Score_1", "Score_2"]

arrayDF = spark.createDataFrame(data = array_data, schema=array_schema)

display(arrayDF)


# COMMAND ----------

# DBTITLE 1,Convert Sample Data Frame into Array Data Frame
from pyspark.sql import functions as F

masterDF = arrayDF.groupby("Name").agg(F.collect_list('Score_1').alias('Array_score_1'),F.collect_list('Score_2').alias('Array_score_2'))

display(masterDF)
masterDF.printSchema()

# COMMAND ----------

# DBTITLE 1,Apply array_zip function on array DF
arr_zip_df = masterDF.withColumn('Zipped_value', F.arrays_zip("Array_Score_1","Array_Score_2"))

arr_zip_df.show(10,False)
