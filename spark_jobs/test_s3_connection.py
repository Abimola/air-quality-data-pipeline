from pyspark.sql import SparkSession
import os

spark = (
    SparkSession.builder
    .appName("S3BucketList")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
    .getOrCreate()
)

# 👇 Add this snippet to inspect the active S3A configs
for k, v in spark.sparkContext._conf.getAll():
    if "s3a" in k:
        print(k, "=", v)

# Then try to read your bucket
try:
    df = spark.read.text("s3a://aq-pipeline/")
    print("✅ Successfully accessed S3 bucket!")
except Exception as e:
    print("❌ Failed to access bucket:", e)

spark.stop()
