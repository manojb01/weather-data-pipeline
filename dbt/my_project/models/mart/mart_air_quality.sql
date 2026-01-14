{{
    config(
        materialized='table'
    )
}}

-- Air quality analysis and trends
with air_quality_data as (
    select
        city,
        co,
        no2,
        o3,
        so2,
        pm2_5,
        pm10,
        us_epa_index,
        gb_defra_index,
        inserted_at,
        date(inserted_at) as measurement_date
    from {{ ref('stg_weather_data') }}
    where co is not null
        or no2 is not null
        or o3 is not null
        or so2 is not null
        or pm2_5 is not null
        or pm10 is not null
),

latest_reading as (
    select
        city,
        co as latest_co,
        no2 as latest_no2,
        o3 as latest_o3,
        so2 as latest_so2,
        pm2_5 as latest_pm2_5,
        pm10 as latest_pm10,
        us_epa_index as latest_us_epa_index,
        gb_defra_index as latest_gb_defra_index,
        inserted_at as last_measurement_time,
        row_number() over (partition by city order by inserted_at desc) as rn
    from air_quality_data
),

daily_averages as (
    select
        city,
        measurement_date,
        avg(co) as avg_co,
        avg(no2) as avg_no2,
        avg(o3) as avg_o3,
        avg(so2) as avg_so2,
        avg(pm2_5) as avg_pm2_5,
        avg(pm10) as avg_pm10,
        avg(us_epa_index) as avg_us_epa_index,
        avg(gb_defra_index) as avg_gb_defra_index
    from air_quality_data
    group by city, measurement_date
)

select
    l.city,
    l.latest_co,
    l.latest_no2,
    l.latest_o3,
    l.latest_so2,
    l.latest_pm2_5,
    l.latest_pm10,
    l.latest_us_epa_index,
    l.latest_gb_defra_index,
    case
        when l.latest_us_epa_index = 1 then 'Good'
        when l.latest_us_epa_index = 2 then 'Moderate'
        when l.latest_us_epa_index = 3 then 'Unhealthy for Sensitive Groups'
        when l.latest_us_epa_index = 4 then 'Unhealthy'
        when l.latest_us_epa_index = 5 then 'Very Unhealthy'
        when l.latest_us_epa_index = 6 then 'Hazardous'
        else 'Unknown'
    end as air_quality_status,
    l.last_measurement_time,
    d.avg_co as daily_avg_co,
    d.avg_no2 as daily_avg_no2,
    d.avg_o3 as daily_avg_o3,
    d.avg_so2 as daily_avg_so2,
    d.avg_pm2_5 as daily_avg_pm2_5,
    d.avg_pm10 as daily_avg_pm10
from latest_reading l
left join daily_averages d
    on l.city = d.city
    and date(l.last_measurement_time) = d.measurement_date
where l.rn = 1
