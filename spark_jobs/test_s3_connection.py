import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv


project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(project_root, ".env"))
BUCKET = os.getenv("S3_BUCKET_NAME")

spark = SparkSession.builder \
    .appName("S3Test") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()

BUCKET = "your-bucket-name"
print("🔍 Listing files in:", BUCKET)

df = spark.read.text(f"s3a://{BUCKET}/raw/openaq/")  # should succeed if S3 + creds work
print("✅ Successfully read S3 folder, row count:", df.count())

spark.stop()
