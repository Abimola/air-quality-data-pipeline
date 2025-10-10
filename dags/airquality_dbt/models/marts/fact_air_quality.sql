{{ config(
    materialized='incremental',
    unique_key=['station_id', 'sensor_id', 'weather_timestamp']
) }}

select
    ba.station_id,
    ba.sensor_id,
    ba.parameter_value,
    ba.parameter_measurement_time,
    ba.sensor_status,
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
    ba.weather_timestamp,
    make_timestamp(ba.year, ba.month, ba.day, ba.hour, 0, 0) as ingestion_time
from {{ ref('base_air_quality') }} as ba

{% if is_incremental() %}
  where ba.weather_timestamp > (select max(weather_timestamp) from {{ this }})
{% endif %}