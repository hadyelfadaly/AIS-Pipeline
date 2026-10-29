-- One row per vessel per report time. Weather stays NULL when Spark had no
-- weather for that cell and hour: treating missing weather as calm water would
-- bias exactly the question this project asks.

with source as (
    select * from {{ source('raw', 'ais_positions') }}
),

deduplicated as (
    select *
    from source
    qualify row_number() over (
        partition by user_id, event_time
        order by polled_at desc nulls last
    ) = 1
)

select
    -- identifiers and time
    user_id                         as vessel_mmsi,
    trim(name)                      as vessel_name,
    event_time,
    event_time::date                as event_date,
    hour(event_time)                as event_hour,

    -- position and movement
    latitude,
    longitude,
    sog                             as speed_knots,
    cog                             as course_degrees,
    navigational_status,
    case navigational_status
        when 0  then 'Under way using engine'
        when 1  then 'At anchor'
        when 2  then 'Not under command'
        when 3  then 'Restricted manoeuvrability'
        when 4  then 'Constrained by draught'
        when 5  then 'Moored'
        when 6  then 'Aground'
        when 7  then 'Engaged in fishing'
        when 8  then 'Under way sailing'
        else 'Not defined'
    end                             as navigation_status_desc,

    -- weather (Open-Meteo, joined in Spark on weather cell + hour)
    cell_id                         as weather_cell_id,
    wave_height_m,
    wind_speed_kmh,
    current_velocity_kmh,
    (wave_height_m is not null or wind_speed_kmh is not null) as has_weather,
    case
        when wave_height_m is null and wind_speed_kmh is null then 'Unknown'
        when wave_height_m >= 3.0 or wind_speed_kmh >= 50 then 'Rough'
        when wave_height_m >= 1.5 or wind_speed_kmh >= 30 then 'Moderate'
        else 'Calm'
    end                             as sea_state

from deduplicated