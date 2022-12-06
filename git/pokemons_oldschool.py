from pyspark.sql import SparkSession

spark = SparkSession.builder.master("yarn").appName("MyApp").getOrCreate()


print('success')