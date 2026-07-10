import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test").getOrCreate()
print("BRONZE CDC COUNT:")
df = spark.read.format("iceberg").load("dataone.bronze.orders_cdc")
print(df.count())
