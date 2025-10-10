{{ config(materialized='table') }}

select distinct
    sensor_id,
    parameter_name,
    display_name,
    units
from {{ ref('base_air_quality') }}
where sensor_id is not null
