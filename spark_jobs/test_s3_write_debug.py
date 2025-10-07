from pyspark.sql import SparkSession
import os

bucket = os.getenv("S3_BUCKET_NAME")
region = os.getenv("AWS_DEFAULT_REGION", "eu-west-2")

spark = (
    SparkSession.builder
    .appName("S3WriteDebug")
    .master("spark://spark-master:7077")
    .config("spark.executor.memory", "512m")
    .config("spark.driver.memory", "512m")
    .config("spark.executor.cores", "1")
    .config("spark.hadoop.fs.s3a.endpoint", f"s3.{region}.amazonaws.com")
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("INFO")

df = spark.createDataFrame([(1, "debug"), (2, "test")], ["id", "text"])
output_path = f"s3a://{bucket}/spark_debug_output/"

print(f"📤 Writing to {output_path}")
df.write.mode("overwrite").parquet(output_path)

print("✅ Write complete!")
spark.stop()
