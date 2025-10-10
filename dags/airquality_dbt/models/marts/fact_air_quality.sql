{{ config(materialized='table') }}

select
    ba.station_id,
    ba.sensor_id,
    ba.parameter_measurement_time,
    ba.parameter_value,
    ba.temperature,
    ba.apparent_temperature,
    ba.humidity,
    ba.pressure,
    ba.dew_point,
    ba.uvi,
    ba.clouds,
    ba.wind_speed,
    ba.wind_deg,
    ba.wind_gust,
    make_timestamp(ba.year, ba.month, ba.day, ba.hour, 0, 0) as ingestion_time
from {{ ref('base_air_quality') }} as ba
