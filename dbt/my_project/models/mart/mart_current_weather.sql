{{
    config(
        materialized='table'
    )
}}

-- Latest weather snapshot for each city
with latest_records as (
    select
        city,
        country,
        region,
        latitude,
        longitude,
        temperature,
        feelslike,
        weather_descriptions,
        humidity,
        wind_speed,
        wind_dir,
        pressure,
        visibility,
        uv_index,
        cloudcover,
        precip,
        inserted_at,
        row_number() over (partition by city order by inserted_at desc) as rn
    from {{ ref('stg_weather_data') }}
)

select
    city,
    country,
    region,
    latitude,
    longitude,
    temperature,
    feelslike,
    temperature - feelslike as temp_feels_diff,
    weather_descriptions,
    humidity,
    wind_speed,
    wind_dir,
    pressure,
    visibility,
    uv_index,
    cloudcover,
    precip,
    inserted_at as last_updated
from latest_records
where rn = 1
