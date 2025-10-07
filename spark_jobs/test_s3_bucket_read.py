from pyspark.sql import SparkSession
import os

spark = (
    SparkSession.builder.appName("S3BucketList")
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000")          
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
    .config("spark.hadoop.fs.s3a.attempts.maximum", "3")
    .getOrCreate()
)

hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.connection.timeout", "60000")
hadoop_conf.set("fs.s3a.connection.establish.timeout", "60000")
hadoop_conf.set("fs.s3a.attempts.maximum", "3")
hadoop_conf.set("fs.s3a.connection.maximum", "100")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")



bucket = os.getenv("S3_BUCKET_NAME")

try:
    print(f"🪣 Connected to bucket: {bucket}")
    df = spark.read.text(f"s3a://{bucket}/")
    df.show(5)
except Exception as e:
    print(f"❌ Failed to access bucket: {e}")

spark.stop()
