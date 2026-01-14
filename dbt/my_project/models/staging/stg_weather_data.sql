with source as (
    select * from {{ source('dev', 'raw_weather_data') }}
),

renamed as (
    select
        id,
        city,
        country,
        region,
        latitude,
        longitude,
        timezone_id,
        local_time,
        observation_time,
        temperature,
        weather_code,
        weather_descriptions,
        weather_icon_url,
        wind_speed,
        wind_degree,
        wind_dir,
        pressure,
        precip,
        humidity,
        cloudcover,
        feelslike,
        uv_index,
        visibility,
        sunrise,
        sunset,
        moon_phase,
        co,
        no2,
        o3,
        so2,
        pm2_5,
        pm10,
        us_epa_index,
        gb_defra_index,
        inserted_at
    from source
)

select * from renamed
