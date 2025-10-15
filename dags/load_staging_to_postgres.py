"""
DAG: load_staging_to_postgres
Description:
    Load processed Parquet data from S3 (staging) into Postgres.
    Typically triggered after EMR transform DAG finishes.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import awswrangler as wr
import pandas as pd
from sqlalchemy import text
import boto3
import pg8000
import os


# Configuration
BUCKET = os.getenv("S3_BUCKET_NAME")

# Load database credentials securely from environment variables
PG_HOST = "postgres"
PG_PORT = 5432
PG_DB = os.getenv("POSTGRES_DB")
PG_USER = os.getenv("POSTGRES_USER")
PG_PASS = os.getenv("POSTGRES_PASSWORD")

TARGET_SCHEMA = "airquality_dwh"
TARGET_TABLE = "stg_air_quality"

STAGING_PATH = f"s3://{BUCKET}/staging/air_quality/"

default_args = {
    "owner": "airflow"
}

# Function: Load Parquet from S3 → Postgres
def load_staging_to_postgres(**context):
    context = get_current_context()
    run_hour = context["dag_run"].conf.get("run_hour", None)

    if run_hour:
        dt = datetime.strptime(run_hour, "%Y%m%dT%H%M%S")
        y, m, d, h = dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d"), dt.strftime("%H")
        s3_path = f"{STAGING_PATH}year={y}/month={m}/day={d}/hour={h}/"
        print(f"Loading specific partition: {s3_path}")
        df = wr.s3.read_parquet(path=s3_path)
        df["year"] = y
        df["month"] = m
        df["day"] = d
        df["hour"] = h
    else:
        print(f"Loading all staging data from {STAGING_PATH}")
        df = wr.s3.read_parquet(path=STAGING_PATH, dataset=True)

    if df.empty:
        print("⚠️ No data found — nothing to load.")
        return

    # Convert timestamp columns
    if "measurement_time" in df.columns:
        df["measurement_time"] = pd.to_datetime(df["measurement_time"], errors="coerce")
    if "weather_timestamp" in df.columns:
        df["weather_timestamp"] = pd.to_datetime(df["weather_timestamp"], unit="s", errors="coerce")

    # Connect securely using pg8000
    conn = pg8000.connect(
        user=PG_USER,
        password=PG_PASS,
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DB
    )

    # Ensure schema exists before loading
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};")
        conn.commit()
        print(f"✅ Ensured schema '{TARGET_SCHEMA}' exists in Postgres.")

    # Load data into Postgres
    wr.postgresql.to_sql(
        df=df,
        con=conn,
        schema=TARGET_SCHEMA,
        table=TARGET_TABLE,
        mode="append",
        index=False
    )

    conn.close()
    print(f"✅ Loaded {len(df)} rows into {TARGET_SCHEMA}.{TARGET_TABLE}")


# DAG definition
with DAG(
    dag_id="load_staging_to_postgres",
    description="Load S3 Parquet (staging) into Postgres",
    schedule_interval=None,
    start_date=datetime(2025, 10, 1),
    catchup=False,
    default_args=default_args,
    tags=["s3", "postgres", "load"],
) as dag:

    load_task = PythonOperator(
        task_id="load_parquet_to_pg",
        python_callable=load_staging_to_postgres,
        provide_context=True,
    )

    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_models",
        trigger_dag_id="run_dbt_models",  
        wait_for_completion=False,        
    )

    load_task >> trigger_dbt


