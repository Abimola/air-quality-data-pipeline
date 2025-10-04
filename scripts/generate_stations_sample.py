import os
import requests
import random
import json
import boto3

OPENAQ_API_KEY = os.getenv("OPENAQ_API_KEY")
BUCKET = os.getenv("S3_BUCKET_NAME")
REGION = os.getenv("AWS_DEFAULT_REGION")


def fetch_locations(n=20):
    """Fetch all UK OpenAQ stations, then sample n of them randomly."""
    url = "https://api.openaq.org/v3/locations"
    headers = {"X-API-Key": OPENAQ_API_KEY}
    params = {"countries_id": 79, "limit": 100, "page": 1}
    results = []

    while True:
        r = requests.get(url, headers=headers, params=params)
        r.raise_for_status()
        data = r.json()

        if not data["results"]:
            print("No locations found in UK")
            break

        results.extend(data["results"])
        if len(data["results"]) < params["limit"]:
            break
        params["page"] += 1

    print(f"Found {len(results)} stations in UK")

    sampled = random.sample(results, min(n, len(results)))
    return sampled


def save_to_s3(data, key):
    s3 = boto3.client("s3", region_name=REGION)
    s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(data))
    print(f"Saved station sample to s3://{BUCKET}/{key}")


if __name__ == "__main__":
    stations = fetch_locations(n=20)
    save_to_s3(stations, "config/stations_sample.json")
