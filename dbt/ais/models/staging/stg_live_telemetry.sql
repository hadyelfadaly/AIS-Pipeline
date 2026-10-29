{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw', 'live_telemetry') }}
),

cleaned as (
    select
        -- Identifiers
        user_id as vessel_mmsi,
        event_time,
        date(event_time) as event_date,
        year(event_time) as event_year,
        month(event_time) as event_month,
        hour(event_time) as event_hour,

        -- Vessel Information
        trim(name) as vessel_name,
        ship_type,
        case
            when ship_type between 70 and 79 then 'Cargo'
            when ship_type between 80 and 89 then 'Tanker'
            when ship_type between 60 and 69 then 'Passenger'
            when ship_type between 50 and 59 then 'Special Craft / Pilot / Tug'
            when ship_type = 30 then 'Fishing'
            when ship_type between 36 and 37 then 'Sailing / Pleasure Craft'
            else 'Other / Unknown'
        end as vessel_category,
        trim(destination) as destination_reported,

        -- Navigation & Telemetry
        latitude,
        longitude,
        sog as speed_knots,
        cog as course_degrees,
        navigational_status,
        case navigational_status
            when 0 then 'Under way using engine'
            when 1 then 'At anchor'
            when 2 then 'Not under command'
            when 3 then 'Restricted maneuverability'
            when 5 then 'Moored'
            when 8 then 'Under way sailing'
            else 'Unknown'
        end as navigation_status_desc,

        -- Weather Conditions
        cell_id as weather_grid_cell_id,
        coalesce(wave_height_m, 0) as wave_height_m,
        coalesce(current_velocity_kmh, 0) as current_velocity_kmh,
        coalesce(wind_speed_kmh, 0) as wind_speed_kmh

    from source
    where user_id is not null
      and event_time is not null
      and latitude between -90 and 90
      and longitude between -180 and 180
)

select * from cleaned
