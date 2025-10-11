{{ config(materialized='table') }}

select distinct
    sensor_id,
    parameter_name,
    display_name,
    units,
    parameter_category
from {{ ref('base_air_quality') }}
where sensor_id is not null
