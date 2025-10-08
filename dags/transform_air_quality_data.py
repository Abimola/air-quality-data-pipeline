"""
DAG: transform_air_quality_data
Description:
    Trigger EMR Serverless Spark job to transform raw OpenAQ and OpenWeatherMap
    data stored in S3 into structured Parquet.

    This DAG is automatically triggered by the `api_ingestion` DAG after
    successful completion. It launches a Spark job on EMR Serverless using
    the script deployed in S3 by GitHub Actions.
"""

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from datetime import datetime
import boto3  


# Retrieve secure configuration from AWS SSM Parameter Store
ssm = boto3.client("ssm", region_name="eu-north-1")
app_id = ssm.get_parameter(Name="/airquality/config/emr-app-id")["Parameter"]["Value"]
role_arn = ssm.get_parameter(Name="/airquality/config/emr-role-arn")["Parameter"]["Value"]
bucket = ssm.get_parameter(Name="/airquality/config/s3-bucket-name")["Parameter"]["Value"]


# 
# DAG CONFIGURATION
# 
# This DAG has no schedule — it runs only when triggered by api_ingestion.
# It starts an EMR Serverless Spark job and writes logs + output to S3.
# 

with DAG(
    dag_id="transform_air_quality_data",
    description="Run EMR Serverless Spark job for air quality data transformation",
    schedule_interval=None,   # Triggered by ingestion DAG
    start_date=datetime(2025, 10, 1),
    catchup=False,
    tags=["emr", "spark", "airquality"],
) as dag:
    

    # Capture the runtime parameter (hour folder) from the triggering DAG
    from airflow.operators.python import get_current_context
    context = get_current_context()
    run_hour = context["dag_run"].conf.get("run_hour", None)
   
    # 
    # EMR Serverless Spark Transformation Task
    # 
    emr_transform_task = EmrServerlessStartJobOperator(
        task_id="run_emr_transform",

        # EMR Serverless application details (fetched securely from SSM)
        application_id=app_id,
        execution_role_arn=role_arn,

        # Spark job details (bucket name dynamically injected)
        job_driver={
            "sparkSubmit": {
                "entryPoint": f"s3://{bucket}/code/spark_jobs/transform_air_quality.py",
                "sparkSubmitParameters": f"--conf spark.run_hour={run_hour}"
            }
        },

        # Logging configuration (S3 bucket dynamically injected)
        configuration_overrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": f"s3://{bucket}/logs/emr-serverless/"
                }
            }
        },

        # AWS connection and region
        aws_conn_id="aws_default",
        region_name="eu-north-1",
    )