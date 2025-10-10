{{ config(materialized='table') }}

select distinct
    station_id,
    station_name,
    latitude,
    longitude
from {{ ref('base_air_quality') }}
where station_id is not null
