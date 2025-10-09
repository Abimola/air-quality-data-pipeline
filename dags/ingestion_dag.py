import os
import json
import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from ingestion import fetch_openaq_latest, fetch_weather, save_to_s3
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils import timezone

# Load secrets from environment variables
STATION_FILE_KEY = "config/stations_sample.json"
BUCKET = os.getenv("S3_BUCKET_NAME")
REGION = os.getenv("AWS_DEFAULT_REGION")
OPENAQ_API_KEY = os.getenv("OPENAQ_API_KEY")
OWM_API_KEY = os.getenv("OWM_API_KEY")


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def load_station_sample():
    """Load the station sample from S3."""
    s3 = boto3.client("s3", region_name=REGION)
    obj = s3.get_object(Bucket=BUCKET, Key=STATION_FILE_KEY)
    return json.loads(obj["Body"].read())


def run_ingestion():
    # Load station list
    locations = load_station_sample()

    for loc in locations:
        loc_id = loc["id"]
        coords = loc.get("coordinates", {})
        lat = coords.get("latitude")
        lon = coords.get("longitude")

        if not lat or not lon:
            print(f"Skipping station {loc_id} - missing coordinates")
            continue

        # Fetch AQ + Weather
        aq = fetch_openaq_latest(loc_id)
        wx = fetch_weather(lat, lon)

        # Save to S3
        save_to_s3(aq, "openaq", loc_id)
        save_to_s3(wx, "weather", loc_id)


with DAG(
    "api_ingestion",
    default_args=default_args,
    description="Fetch random UK AQ + Weather (20 stations) hourly",
    schedule_interval="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    ingest_task = PythonOperator(
        task_id="fetch_and_save_data",
        python_callable=run_ingestion,
    )

# Trigger transformation DAG after ingestion completes
trigger_transform = TriggerDagRunOperator(
    task_id="trigger_transform_dag",
    trigger_dag_id="transform_air_quality_data",
    conf={"run_hour": "{{ macros.timezone.utcnow().strftime('%Y%m%dT%H%M%S') }}"},
    wait_for_completion=False,  
)

ingest_task >> trigger_transform
