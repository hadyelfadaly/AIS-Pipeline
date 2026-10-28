{{ config(materialized='table') }}

with telemetry as (
    select * from {{ ref('stg_live_telemetry') }}
),

ports as (
    select 
        port_id, 
        port_name, 
        latitude as port_lat, 
        longitude as port_lon 
    from {{ ref('dim_ports') }}
),

-- Find the nearest port for each vessel ping using Snowflake's HAVERSINE distance
vessel_with_nearest_port as (
    select
        t.*,
        p.port_id as nearest_port_id,
        p.port_name as nearest_port_name,
        haversine(t.latitude, t.longitude, p.port_lat, p.port_lon) as distance_to_port_km,
        row_number() over (
            partition by t.vessel_mmsi, t.event_time 
            order by haversine(t.latitude, t.longitude, p.port_lat, p.port_lon) asc
        ) as rank_port
    from telemetry t
    cross join ports p
)

select
    -- Primary Key
    {{ dbt_utils.generate_surrogate_key(['vessel_mmsi', 'event_time']) }} as telemetry_id,

    -- Foreign Keys
    vessel_mmsi,
    nearest_port_id,
    event_date,
    event_time,

    -- Movement & Navigation
    latitude,
    longitude,
    speed_knots,
    course_degrees,
    navigation_status_desc,

    -- Proximity to Port
    nearest_port_name,
    round(distance_to_port_km, 2) as distance_to_port_km,
    case when distance_to_port_km <= 25.0 then true else false end as is_in_port_anchorage_zone,

    -- Weather Impact
    wave_height_m,
    wind_speed_kmh,
    current_velocity_kmh,
    case 
        when wave_height_m >= 3.0 or wind_speed_kmh >= 50.0 then 'Rough / Hazardous Sea'
        when wave_height_m >= 1.5 or wind_speed_kmh >= 30.0 then 'Moderate Sea'
        else 'Calm Sea'
    end as sea_condition_category

from vessel_with_nearest_port
where rank_port = 1
