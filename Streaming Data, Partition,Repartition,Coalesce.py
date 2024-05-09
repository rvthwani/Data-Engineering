# Databricks notebook source
# DBTITLE 1,Define Schema for Sample Streaming data
from pyspark.sql.types import StructType,StructField,IntegerType,StringType

schema_defind = StructType([StructField('File', StringType(),True),
                            StructField('Shop', StringType(),True),
                            StructField('Sale_count', IntegerType(),True)
                            ])

# COMMAND ----------

# DBTITLE 1,Create folder structure in DBFS file system
dbutils.fs.mkdirs("Filestore/tables/stream_checkpoint/")
dbutils.fs.mkdirs("Filestore/tables/stream_read/")
dbutils.fs.mkdirs("Filestore/tables/stream_write/")

# COMMAND ----------

# DBTITLE 1,Read Streaming Data
df = spark.readStream.format("csv").schema(schema_defind).option("header",True).option("sep",";").load("/FileStore/tables/stream_read/")

df1 = df.groupBy("shop").sum("sale_count")
display(df1)

# COMMAND ----------

# DBTITLE 1,Write Streaming Data
df4 = df.writeStream.format("parquet").outputMode("append").option("path", "/Filestore/tables/stream_write/").option("checkpointLocation", "/Filestore/tables/stream_checkpoint/").start().awaitTermination()

# COMMAND ----------

# DBTITLE 1,Verify the written stream output data
display(spark.read.format("parquet").load("/Filestore/tables/stream_write/*.parquet"))

# COMMAND ----------

# DBTITLE 1,Default Parallelism
sc.defaultParallelism

# COMMAND ----------

spark.conf.get("spark.sql.files.maxPartitionBytes")

# COMMAND ----------

# DBTITLE 1,Generating data within spark environment
from pyspark.sql.types import IntegerType

df = spark.createDataFrame(range(10), IntegerType())

df.rdd.getNumPartitions()#to get the number of partitions

# COMMAND ----------

# DBTITLE 1,Verify the data within all partition
df.rdd.glom().collect()# to get the partitions seggregations

# COMMAND ----------

# DBTITLE 1,Read External File in Spark
dbutils.fs.ls("/Filestore/tables/baby_names")

# COMMAND ----------

df = spark.read.format("csv").option("Inferschema", True).option("header", True).option("sep", ",").load("/Filestore/tables/baby_names")
df.rdd.getNumPartitions()

# COMMAND ----------

spark.conf.set("spark.sql.files.maxPartitionBytes", 200000)
spark.conf.get("spark.sql.files.maxPartitionBytes")

# COMMAND ----------

df = spark.read.format("csv").option("Inferschema", True).option("header", True).option("sep", ",").load("/Filestore/tables/baby_names")
df.rdd.getNumPartitions()

# COMMAND ----------

# DBTITLE 1,Repartition
# from pyspark.sql.types import IntegerType

# df = spark.createDataFrame(range(10), IntegerType())

# df.rdd.getNumPartitions()#to get the number of partitions
# display(df)
df.rdd.glom().collect()

# COMMAND ----------

df1 = df.repartition(20)
df1.rdd.getNumPartitions()

# COMMAND ----------

df1.rdd.glom().collect()

# COMMAND ----------

df1=df.repartition(2)
df1.rdd.getNumPartitions()

# COMMAND ----------

df1.rdd.glom().collect()

# COMMAND ----------

# DBTITLE 1,Coalesce
df2 = df.coalesce(2)
df2.rdd.getNumPartitions()

df2.rdd.glom().collect()
