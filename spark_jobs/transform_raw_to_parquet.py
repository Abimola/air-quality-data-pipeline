"""
Spark ETL job for transforming raw OpenAQ and OpenWeatherMap JSON data stored in S3
into a unified, structured Parquet dataset.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, regexp_extract, input_file_name
from datetime import datetime
import boto3


# 1. Retrieve S3 bucket name securely from AWS Systems Manager Parameter Store
ssm = boto3.client("ssm", region_name="eu-north-1")
param = ssm.get_parameter(Name="/airquality/config/s3-bucket-name")
bucket = param["Parameter"]["Value"]

print(f"✅ Using bucket: {bucket}")


# 2. Initialize Spark session
spark = (
    SparkSession.builder
    .appName("TransformRawToParquet")
    .getOrCreate()
)


# 3. Get the run_hour parameter from Airflow
run_hour = spark.conf.get("spark.run_hour", None)
if run_hour:
    run_dt = datetime.strptime(run_hour, "%Y%m%dT%H%M%S")
    year_str = run_dt.strftime("%Y")
    month_str = run_dt.strftime("%m")
    day_str = run_dt.strftime("%d")
    hour_str = run_dt.strftime("%H")

    # Only read the hour’s raw data
    RAW_OPENAQ = f"s3://{bucket}/raw/openaq/{year_str}/{month_str}/{day_str}/{hour_str}/"
    RAW_WEATHER = f"s3://{bucket}/raw/weather/{year_str}/{month_str}/{day_str}/{hour_str}/"
    print(f"✅ Processing data for {year_str}-{month_str}-{day_str} hour {hour_str}")
else:
    # Fallback: process all data (e.g., manual run)
    RAW_OPENAQ = f"s3://{bucket}/raw/openaq/"
    RAW_WEATHER = f"s3://{bucket}/raw/weather/"
    print("⚠️ No run_hour provided — processing full dataset.")


STATIONS_PATH = f"s3://{bucket}/config/stations_sample.json"
STAGING_PATH = f"s3://{bucket}/staging/air_quality/"


# 4. Read raw JSON data from S3 (filtered by hour if provided)
print("Reading OpenAQ data from S3...")
aq_df = (
    spark.read
    .option("multiLine", True)
    .option("recursiveFileLookup", "true")
    .json(RAW_OPENAQ)
)

print("Reading Weather data from S3...")
wx_df = (
    spark.read
    .option("multiLine", True)
    .option("recursiveFileLookup", "true")
    .json(RAW_WEATHER)
)

print("Reading Stations metadata from S3...")
stations_df = spark.read.option("multiLine", True).json(STATIONS_PATH)


# 5. Flatten and enrich OpenAQ data
try:
    aq_flat = aq_df.withColumn("results", explode("results")).select(
        col("results.locationsId").alias("station_id"),
        col("results.sensorsId").alias("sensor_id"),
        col("results.value").alias("value"),
        col("results.datetime.utc").alias("measurement_time"),
        col("results.coordinates.latitude").alias("latitude"),
        col("results.coordinates.longitude").alias("longitude"),
        regexp_extract(input_file_name(), r"/openaq/(\d{4})/", 1).alias("year"),
        regexp_extract(input_file_name(), r"/openaq/\d{4}/(\d{2})/", 1).alias("month"),
        regexp_extract(input_file_name(), r"/openaq/\d{4}/\d{2}/(\d{2})/", 1).alias("day"),
        regexp_extract(input_file_name(), r"/openaq/\d{4}/\d{2}/\d{2}/(\d{2})/", 1).alias("hour")
    )

    sensors_flat = stations_df.withColumn("sensors", explode("sensors")).select(
        col("id").alias("station_id"),
        col("name").alias("station_name"),
        col("sensors.id").alias("sensor_id"),
        col("sensors.parameter.name").alias("parameter_name"),
        col("sensors.parameter.units").alias("units"),
        col("sensors.parameter.displayName").alias("display_name")
    )

    openaq_enriched = aq_flat.join(sensors_flat, on=["station_id", "sensor_id"], how="left")

    print("✅ OpenAQ successfully flattened and enriched with sensor metadata and ingestion info.")

except Exception as e:
    print(f"⚠️ OpenAQ flattening or enrichment failed: {e}")
    openaq_enriched = aq_df


# 6. Flatten Weather data and extract station_id + ingestion info
try:
    wx_flat = wx_df.select(
        col("current.temp").alias("temperature"),
        col("current.feels_like").alias("feels_like"),
        col("current.humidity").alias("humidity"),
        col("current.pressure").alias("pressure"),
        col("current.dew_point").alias("dew_point"),
        col("current.uvi").alias("uvi"),
        col("current.clouds").alias("clouds"),
        col("current.wind_speed").alias("wind_speed"),
        col("current.wind_deg").alias("wind_deg"),
        col("current.wind_gust").alias("wind_gust"),
        col("current.dt").alias("weather_timestamp"),
        regexp_extract(input_file_name(), r"(\d+)\.json$", 1).alias("station_id"),
        regexp_extract(input_file_name(), r"/weather/(\d{4})/", 1).alias("year"),
        regexp_extract(input_file_name(), r"/weather/\d{4}/(\d{2})/", 1).alias("month"),
        regexp_extract(input_file_name(), r"/weather/\d{4}/\d{2}/(\d{2})/", 1).alias("day"),
        regexp_extract(input_file_name(), r"/weather/\d{4}/\d{2}/\d{2}/(\d{2})/", 1).alias("hour")
    )

    print("✅ Weather data successfully flattened and ingestion info added.")

except Exception as e:
    print(f"⚠️ Weather flattening failed: {e}")
    wx_flat = wx_df


# 7. Join OpenAQ and Weather datasets on station_id + ingestion info
try:
    join_keys = ["station_id", "year", "month", "day", "hour"]

    final_df = (
        openaq_enriched
        .join(wx_flat, on=join_keys, how="left")
        .select(*openaq_enriched.columns, *[c for c in wx_flat.columns if c not in join_keys])
    )

    print("✅ Successfully joined OpenAQ and Weather data on station_id + year/month/day/hour.")
except Exception as e:
    print(f"⚠️ Failed to join AQ and Weather data: {e}")
    final_df = openaq_enriched


# 8 Enforce consistent column data types for Parquet schema
try:
    print("Enforcing consistent data types before writing to Parquet...")

    type_mapping = {
        "station_id": "string",
        "sensor_id": "string",
        "value": "double",
        "measurement_time": "string",
        "latitude": "double",
        "longitude": "double",
        "parameter_name": "string",
        "units": "string",
        "display_name": "string",
        "station_name": "string",
        "temperature": "double",
        "feels_like": "double",
        "humidity": "double",
        "pressure": "double",
        "dew_point": "double",
        "uvi": "double",
        "clouds": "double",
        "visibility": "double",
        "wind_speed": "double",
        "wind_deg": "double",
        "wind_gust": "double"
    }

    # Keep track of missing columns
    existing_columns = set(final_df.columns)
    missing_columns = [col for col in type_mapping.keys() if col not in existing_columns]

    # Apply type casting safely for existing columns
    for column, dtype in type_mapping.items():
        if column in existing_columns:
            final_df = final_df.withColumn(column, col(column).cast(dtype))

    print("✅ All column data types standardized successfully.")

    # Log any skipped columns
    if missing_columns:
        print(f"✅ Skipped missing columns (not found in DataFrame): {', '.join(missing_columns)}")

    # Verify final schema
    print("\n Final DataFrame Schema:")
    final_df.printSchema()

except Exception as e:
    print(f"⚠️ Failed to standardize data types: {e}")


# 9. Write unified dataset to S3 as Parquet (partitioned by ingestion date/time)
try:
    print(f"Writing unified dataset to {STAGING_PATH} ...")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    final_df.write.mode("overwrite").partitionBy("year", "month", "day", "hour").parquet(STAGING_PATH)

    print("✅ Transformation complete — data successfully written to S3.")
except Exception as e:
    print(f"⚠️ Failed to write Parquet output: {e}")


spark.stop()
