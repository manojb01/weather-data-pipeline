{{
    config(
        materialized='table'
    )
}}

-- Daily aggregated weather metrics
with daily_data as (
    select
        city,
        date(inserted_at) as weather_date,
        avg(temperature) as avg_temperature,
        max(temperature) as max_temperature,
        min(temperature) as min_temperature,
        avg(feelslike) as avg_feelslike,
        avg(humidity) as avg_humidity,
        max(humidity) as max_humidity,
        min(humidity) as min_humidity,
        avg(wind_speed) as avg_wind_speed,
        max(wind_speed) as max_wind_speed,
        avg(pressure) as avg_pressure,
        sum(precip) as total_precipitation,
        avg(cloudcover) as avg_cloudcover,
        avg(visibility) as avg_visibility,
        avg(uv_index) as avg_uv_index,
        count(*) as observation_count,
        min(inserted_at) as first_observation,
        max(inserted_at) as last_observation
    from {{ ref('stg_weather_data') }}
    group by city, date(inserted_at)
)

select
    city,
    weather_date,
    avg_temperature,
    max_temperature,
    min_temperature,
    max_temperature - min_temperature as temperature_range,
    avg_feelslike,
    avg_humidity,
    max_humidity,
    min_humidity,
    avg_wind_speed,
    max_wind_speed,
    avg_pressure,
    total_precipitation,
    avg_cloudcover,
    avg_visibility,
    avg_uv_index,
    observation_count,
    first_observation,
    last_observation
from daily_data
order by city, weather_date desc
