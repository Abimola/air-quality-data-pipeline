"""
DAG: run_dbt_models
Description:
    Runs dbt models and tests after data has been loaded into Postgres.
    This ensures dbt transformations and quality checks are part of the pipeline.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False
}


# DAG definition
with DAG(
    dag_id="run_dbt_models",
    default_args=default_args,
    start_date=datetime(2025, 10, 1),
    schedule_interval=None, 
    catchup=False,
    tags=["dbt", "postgres", "analytics"],
) as dag:

    # Run dbt models
    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command=(
            "cd /opt/airflow/dags/airquality_dbt && dbt run"
        ),
    )

    # Run dbt tests
    test_dbt = BashOperator(
        task_id="test_dbt",
        bash_command=(
            "cd /opt/airflow/dags/airquality_dbt && dbt test"
        ),
    )

    run_dbt >> test_dbt

