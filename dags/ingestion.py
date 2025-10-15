import os
import requests
import boto3
import json
import random
from datetime import datetime



# Load secrets from environment variables
BUCKET = os.getenv("S3_BUCKET_NAME")
REGION = os.getenv("AWS_DEFAULT_REGION")
OPENAQ_API_KEY = os.getenv("OPENAQ_API_KEY")
OWM_API_KEY = os.getenv("OWM_API_KEY")



# Fetch the latest AQ reading for a given station.
def fetch_openaq_latest(loc_id):
    url = f"https://api.openaq.org/v3/locations/{loc_id}/latest"
    headers = {"X-API-Key": OPENAQ_API_KEY}
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json()


# Fetch current weather at the station's location.
def fetch_weather(lat, lon):
    url = "https://api.openweathermap.org/data/3.0/onecall"
    params = {
        "lat": lat,
        "lon": lon,
        "exclude": "minutely,hourly,daily,alerts",
        "appid": OWM_API_KEY
    }
    r = requests.get(url, params=params)
    r.raise_for_status()
    return r.json()

# Save JSON data to S3 in timestamped folders.
def save_to_s3(data, prefix, station_id):
    s3 = boto3.client("s3", region_name=REGION)
    now = datetime.utcnow()
    key = f"raw/{prefix}/{now.strftime('%Y/%m/%d/%H')}/{station_id}.json"

    s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(data))
    print(f"Saved to s3://{BUCKET}/{key}")
