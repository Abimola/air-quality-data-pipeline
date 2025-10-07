from pyspark.sql import SparkSession
import os

# Create a Spark session
spark = SparkSession.builder.appName("S3BucketList").getOrCreate()

# Configure Spark to use AWS S3
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
hadoop_conf.set("fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
hadoop_conf.set("fs.s3a.endpoint", "s3.amazonaws.com")

bucket = os.getenv("S3_BUCKET_NAME")
print(f"🪣 Connected to bucket: {bucket}")

# Try listing files at root
try:
    df = spark.read.text(f"s3a://{bucket}/")
    print(f"✅ Successfully read from s3a://{bucket}/")
    df.show(truncate=False)
except Exception as e:
    print(f"❌ Failed to access bucket: {e}")

spark.stop()
