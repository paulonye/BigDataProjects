from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder.appName("LoadData").getOrCreate()


#df1 = spark.read.option("header", "true").parquet(url)
df1 = spark.read.option("header", "true").parquet("/Users/macbook/Desktop/test.parquet")

print(df1.count())



