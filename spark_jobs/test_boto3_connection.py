import boto3, os

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_DEFAULT_REGION")
)

# list buckets
for b in s3.list_buckets()["Buckets"]:
    print(b["Name"])

# upload a local file
s3.upload_file("/opt/spark/data/test.json", os.getenv("S3_BUCKET_NAME"), "raw/test.json")
