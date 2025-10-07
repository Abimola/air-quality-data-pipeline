from pyspark.sql import SparkSession
import os

# Load environment variables securely
bucket = os.getenv("S3_BUCKET_NAME")  
region = os.getenv("AWS_DEFAULT_REGION", "eu-west-2")

# Build Spark session
spark = (
    SparkSession.builder
    .appName("S3WriteTest")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)


# Create a small DataFrame
data = [("Spark → S3 write test succeeded!",)]
df = spark.createDataFrame(data, ["message"])

# Write test
output_path = f"s3a://{bucket}/spark_write_test/"

try:
    print(f"\n📤 Writing DataFrame to: {output_path} ...")
    df.write.mode("overwrite").parquet(output_path)
    print("✅ Successfully wrote test data to S3!")
except Exception as e:
    print("❌ Failed to write to S3:", e)

spark.stop()
