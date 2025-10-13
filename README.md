# global-air-quality-data-pipeline
End-to-end data engineering pipeline: hourly ingestion of air quality &amp; weather data into AWS S3, transformed with Spark, modeled with dbt, orchestrated with Airflow, and visualized in Metabase.


```mermaid
flowchart TD
    A[OpenAQ API] -->|JSON| B[S3 Raw]
    A2[OpenWeatherMap API] -->|JSON| B
    B --> C[EMR Serverless (PySpark)]
    C --> D[S3 Staging]
    D --> E[Airflow Load DAG]
    E --> F[PostgreSQL Warehouse]
    F --> G[dbt Models]
    G --> H[Metabase Dashboards]
