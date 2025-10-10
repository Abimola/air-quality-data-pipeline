{{ config(materialized='view') }}

select
    station_id,
    sensor_id,
    value::double precision as parameter_value,
    measurement_time::timestamp as parameter_measurement_time,
    case
        when measurement_time < weather_timestamp - interval '1 day' then 'inactive'
        else 'active'
    end as sensor_status,
    -- Latitude and longitude rounded to 4 decimal places
    round(latitude::double precision, 4) as latitude,
    round(longitude::double precision, 4) as longitude,
    station_name,
    parameter_name,
    units,
    display_name,
    -- Convert temperature values from Kelvin to Celsius
    (temperature::double precision - 273.15) as temperature,
    (feels_like::double precision - 273.15) as apparent_temperature,
    humidity::double precision as humidity,
    pressure::double precision as pressure,
    dew_point::double precision as dew_point,
    uvi::double precision as uvi,
    clouds::double precision as clouds,
    wind_speed::double precision as wind_speed,
    wind_deg::double precision as wind_deg,
    wind_gust::double precision as wind_gust,
    weather_timestamp::timestamp as weather_timestamp,
    year::int as year,
    month::int as month,
    day::int as day,
    hour::int as hour
from {{ source('airquality_dwh', 'stg_air_quality') }}
