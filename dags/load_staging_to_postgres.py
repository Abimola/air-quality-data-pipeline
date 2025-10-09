"""
DAG: load_staging_to_postgres
Description:
    Load processed Parquet data from S3 (staging) into Postgres.
    Typically triggered after EMR transform DAG finishes.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from datetime import datetime, timedelta
import awswrangler as wr
import pandas as pd
from sqlalchemy import create_engine, text
import boto3

# -------------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------------
ssm = boto3.client("ssm", region_name="eu-north-1")
bucket = ssm.get_parameter(Name="/airquality/config/s3-bucket-name")["Parameter"]["Value"]

PG_CONN_STR = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
TARGET_SCHEMA = "airquality_dwh"
TARGET_TABLE = "stg_air_quality"

STAGING_PATH = f"s3://{bucket}/staging/air_quality/"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# -------------------------------------------------------------------------
# Function: Load Parquet from S3 → Postgres
# -------------------------------------------------------------------------
def load_staging_to_postgres(**context):
    context = get_current_context()
    run_hour = context["dag_run"].conf.get("run_hour", None)

    if run_hour:
        dt = datetime.strptime(run_hour, "%Y%m%dT%H%M%S")
        y, m, d, h = dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d"), dt.strftime("%H")
        s3_path = f"{STAGING_PATH}year={y}/month={m}/day={d}/hour={h}/"
        print(f"Loading specific partition: {s3_path}")
        df = wr.s3.read_parquet(path=s3_path)
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

    # Ensure schema exists before loading
    engine = create_engine(PG_CONN_STR)
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};"))
        conn.commit()
        print(f"✅ Ensured schema '{TARGET_SCHEMA}' exists in Postgres.")

    # Load data into Postgres
    wr.postgresql.to_sql(
        df=df,
        con=engine,
        schema=TARGET_SCHEMA,
        table=TARGET_TABLE,
        mode="append",   
        index=False
    )

    print(f"✅ Loaded {len(df)} rows into {TARGET_SCHEMA}.{TARGET_TABLE}")


# -------------------------------------------------------------------------
# DAG definition
# -------------------------------------------------------------------------
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


    