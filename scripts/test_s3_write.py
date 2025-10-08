from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("S3WriteTest").getOrCreate()

data = [("EMR Serverless works!",)]
df = spark.createDataFrame(data, ["message"])

# 👇 Replace YOUR_BUCKET with your real bucket name
df.write.mode("overwrite").parquet("s3://aq-pipeline/spark_write_test/")

print("✅ Successfully wrote test data to S3!")
spark.stop()
