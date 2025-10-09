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
from airflow.operators.python import PythonOperator, get_current_context
from datetime import datetime
import boto3


# -------------------------------------------------------------------------
# Retrieve secure configuration from AWS Systems Manager Parameter Store
# -------------------------------------------------------------------------
ssm = boto3.client("ssm", region_name="eu-north-1")

app_id = ssm.get_parameter(Name="/airquality/config/emr-app-id")["Parameter"]["Value"]
role_arn = ssm.get_parameter(Name="/airquality/config/emr-role-arn")["Parameter"]["Value"]
bucket = ssm.get_parameter(Name="/airquality/config/s3-bucket-name")["Parameter"]["Value"]

# -------------------------------------------------------------------------
# DAG CONFIGURATION
# -------------------------------------------------------------------------
# This DAG has no schedule — it runs only when triggered by api_ingestion.
# It starts an EMR Serverless Spark job and writes logs + output to S3.
# -------------------------------------------------------------------------

def start_emr_job(**kwargs):
    """Dynamically retrieves runtime parameters and triggers EMR Serverless Spark job."""
    context = get_current_context()
    run_hour = context["dag_run"].conf.get("run_hour", None)

    # Build the job driver
    job_driver = {
        "sparkSubmit": {
            "entryPoint": f"s3://{bucket}/code/spark_jobs/transform_raw_to_parquet.py",
        }
    }

    # Include run_hour if passed from the triggering DAG
    if run_hour:
        job_driver["sparkSubmit"]["sparkSubmitParameters"] = f"--conf spark.run_hour={run_hour}"

    emr_task = EmrServerlessStartJobOperator(
        task_id="run_emr_transform",
        application_id=app_id,
        execution_role_arn=role_arn,
        name=f"airquality_transform_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
        job_driver=job_driver,
        configuration_overrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": f"s3://{bucket}/logs/emr-serverless/"
                }
            }
        },
        aws_conn_id="aws_default",
        
    )

    # Trigger EMR job directly
    return emr_task.execute(context=context)


# -------------------------------------------------------------------------
# DAG DEFINITION
# -------------------------------------------------------------------------
with DAG(
    dag_id="transform_air_quality_data",
    description="Run EMR Serverless Spark job for air quality data transformation",
    schedule_interval=None,
    start_date=datetime(2025, 10, 1),
    catchup=False,
    tags=["emr", "spark", "airquality"],
) as dag:

    emr_transform_task = PythonOperator(
        task_id="trigger_emr_transform",
        python_callable=start_emr_job,
    )
