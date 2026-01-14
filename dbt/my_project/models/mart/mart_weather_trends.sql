{{
    config(
        materialized='table'
    )
}}

-- Weather trends over time (hourly/recent observations)
with weather_time_series as (
    select
        city,
        temperature,
        feelslike,
        humidity,
        wind_speed,
        wind_dir,
        pressure,
        cloudcover,
        precip,
        visibility,
        weather_descriptions,
        inserted_at,
        lag(temperature) over (partition by city order by inserted_at) as prev_temperature,
        lag(humidity) over (partition by city order by inserted_at) as prev_humidity,
        lag(wind_speed) over (partition by city order by inserted_at) as prev_wind_speed,
        lag(pressure) over (partition by city order by inserted_at) as prev_pressure
    from {{ ref('stg_weather_data') }}
)

select
    city,
    temperature,
    feelslike,
    humidity,
    wind_speed,
    wind_dir,
    pressure,
    cloudcover,
    precip,
    visibility,
    weather_descriptions,
    inserted_at,
    -- Calculate changes from previous observation
    case
        when prev_temperature is not null
        then round((temperature - prev_temperature)::numeric, 2)
        else null
    end as temperature_change,
    case
        when prev_humidity is not null
        then humidity - prev_humidity
        else null
    end as humidity_change,
    case
        when prev_wind_speed is not null
        then round((wind_speed - prev_wind_speed)::numeric, 2)
        else null
    end as wind_speed_change,
    case
        when prev_pressure is not null
        then pressure - prev_pressure
        else null
    end as pressure_change,
    -- Trend indicators
    case
        when prev_temperature is not null and temperature > prev_temperature then 'Rising'
        when prev_temperature is not null and temperature < prev_temperature then 'Falling'
        when prev_temperature is not null then 'Stable'
        else 'Unknown'
    end as temperature_trend
from weather_time_series
order by city, inserted_at desc
