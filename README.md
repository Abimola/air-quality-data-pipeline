# End-to-End Air Quality & Weather Data Pipeline  
**(AWS + Airflow + Spark (EMR) + PostgreSQL + dbt + Metabase + Docker)**

---

## Overview

This project demonstrates a fully automated **modern data engineering pipeline** that collects, processes, models, and visualizes real-time air quality and weather data for monitoring environmental health across stations in the United Kingdom.

It showcases an industry-ready stack integrating **AWS, Airflow, Spark, dbt, PostgreSQL, and Metabase**, all orchestrated and containerized with **Docker** — a complete cloud-based ETL → ELT → BI workflow.

---

## Architecture Diagram

![Data Pipeline](./assets/air_quality_data_pipeline.png)

> *Logos are trademarks of their respective owners and are used here for illustrative purposes only.  
This project is for educational and portfolio demonstration purposes and is not affiliated with or endorsed by any of the mentioned entities.*

---

## Pipeline Overview

| Stage | Tools | Description |
|--------|--------|-------------|
| **Ingestion** | *Apache Airflow, AWS S3* | Airflow DAGs fetch air quality data from **OpenAQ API** and weather data from **OpenWeather API**, storing hourly JSON dumps in Amazon S3. |
| **Transformation** | *Apache Spark (AWS EMR)* | Spark jobs running on Amazon EMR transform raw JSON data into structured Parquet files, standardizing the schema. |
| **Loading & Modelling** | *PostgreSQL, dbt* | Clean Parquet data is loaded into a PostgreSQL warehouse, where dbt performs modular transformations and builds a **star schema** (Fact + Dimensions). |
| **Analytics & Visualization** | *Metabase* | Interactive dashboards present insights on air quality, weather correlations, and station health metrics. |
| **Orchestration & Deployment** | *Apache Airflow, Docker, AWS EC2* | Airflow coordinates all DAGs (Ingest → Transform → Load → Model), containerized within Docker on an AWS EC2 instance. |
| **CI/CD Automation** | *GitHub Actions, AWS S3* | Automates deployment of Spark transformation scripts to S3 on every push to `main`, ensuring EMR always executes the latest version of the job. |

> CI/CD automation powered by [GitHub Actions](https://github.com/features/actions), using [S3 Sync Action by Jake Jarvis](https://github.com/jakejarvis/s3-sync-action).

---

## Data Model

![Star Schema](./assets/star_schema.png)

**Schema:**
- **fact_air_quality** — core hourly measurements combining pollutants and weather data  
- **dim_station** — station metadata (name, coordinates, etc.)  
- **dim_sensor** — pollutant and sensor definitions (PM2.5, NO₂, etc.)

This design supports flexible queries for station-level and pollutant-level analytics.

---

## Dashboards

All dashboards were built in **Metabase**, connected directly to the PostgreSQL data mart.  
They update automatically as new data is ingested through the pipeline.


### 1️. Weather & Air Quality Dashboard
**Purpose:** Display live weather and pollutant levels for a selected station, refreshed hourly.  
**Key Visuals:**  
- Temperature, Humidity, Wind Speed cards  
- Pollutant concentration tiles (PM2.5, NO₂, O₃)  
- Time-series pollutant trends  

![Weather and Air Quality Dashboard](./assets/weather_air_quality_dashboard.png)

### 2. Air Quality — Last 24 Hours
**Purpose:** Compare average pollutant concentrations across stations for the past 24 hours.  
**Key Visuals:**  
- Bar charts for PM1, PM2.5, PM10, NO₂, O₃  
- Identify the most polluted regions  

![Air Quality - Last 24 hours](./assets/air_quality_24h.png)


### 3. Air Quality vs Weather (Per Station)
**Purpose:** Explore how environmental factors influence pollutant levels.  
**Key Visuals:**  
- Scatter plots comparing PM2.5 and NO₂ against Temperature, Humidity, Pressure, and Wind Speed  

![Air Quality vs Weather (Per Station)](./assets/air_quality_vs_weather.png)

### 4. Station Health & Ingestion Summary
**Purpose:** Track data freshness, ingestion delays, and active sensor availability.  
**Key Visuals:**  
- Station uptime and delay table  
- Active/inactive sensor counts per station  
- Pollutant coverage health bars  

![Station Health and Ingestion Summary](./assets/station_health_summary.png)

---

## Technologies Used

| Category | Tools |
|-----------|-------|
| **Orchestration** | Apache Airflow |
| **Storage** | Amazon S3 |
| **Processing** | AWS EMR (Apache Spark) |
| **Data Modelling** | dbt (Data Build Tool) |
| **Warehouse** | PostgreSQL |
| **Visualization** | Metabase |
| **Deployment** | Docker, AWS EC2 |
| **Source APIs** | OpenAQ, OpenWeather |

---

## Data Licensing & Attribution

- **Air quality data** sourced from [OpenAQ](https://openaq.org).  
  Providers include:
  - **DEFRA (UK)** under the [Open Government Licence v2.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/2/),
  - **AirGradient** under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/),
  - **Clarity** under [CC0 1.0](https://creativecommons.org/publicdomain/zero/1.0/deed.ca),
  - **EEA** under [ODC-BY 1.0](https://opendatacommons.org/licenses/by/1-0/).  
- **Weather data** provided by [OpenWeather](https://openweathermap.org/) under [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/).  
- **Pipeline processing** performed via *Airflow → EMR → PostgreSQL → dbt → Metabase*.

---

## Deployment Notes

- All services (Airflow, PostgreSQL, Metabase) run as **Docker containers** on an AWS EC2 instance.  
- Airflow DAGs are scheduled hourly to automate ingestion, transformation, and dbt model refreshes.  
- Metabase dashboards automatically reflect new data on refresh, providing near real-time visibility.

---

## Key Learnings

- Building and orchestrating an end-to-end ELT data pipeline in the cloud  
- Using **dbt** for modular SQL transformations and incremental updates  
- Managing dependencies, data freshness, and schema design in PostgreSQL  
- Automating analytics delivery through **Metabase**

---

## Outcome

- Fully functional data pipeline running on AWS  
- Automated hourly data refresh from APIs  
- Interactive dashboards for environmental insights  
- Production-style workflow using modern DE tools  

---

**Author:** Abimola Onibi 
[LinkedIn](https://www.linkedin.com/in/abimola-onibi/) | [GitHub](https://github.com/Abimola/)

---
