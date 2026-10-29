-- Cargo and tanker position reports with sea state and nearest port.
--
-- Nearest port without a full cross join: every ping and every port gets a
-- 1-degree grid cell, and a ping is only compared with ports in its own cell
-- and the 8 cells around it. A 25 km port zone always fits inside that window
-- (except very close to the poles or the 180th meridian), so the answer is the
-- same as the cross join at a tiny fraction of the cost.

with positions as (
    select
        *,
        floor(latitude)  as lat_cell,
        floor(longitude) as lon_cell
    from {{ ref('int_cargo_positions') }}
),

ports as (
    select
        port_id,
        port_name,
        latitude  as port_lat,
        longitude as port_lon,
        floor(latitude)  as lat_cell,
        floor(longitude) as lon_cell
    from {{ ref('dim_ports') }}
    where latitude is not null
      and longitude is not null
),

offsets as (
    select column1 as d from values (-1), (0), (1)
),

-- each port is listed under its own cell and its 8 neighbours
port_cells as (
    select
        p.port_id,
        p.port_name,
        p.port_lat,
        p.port_lon,
        p.lat_cell + dlat.d as lat_cell,
        p.lon_cell + dlon.d as lon_cell
    from ports p
    cross join offsets dlat
    cross join offsets dlon
),

nearest_port as (
    select
        pos.vessel_mmsi,
        pos.event_time,
        pc.port_id   as nearest_port_id,
        pc.port_name as nearest_port_name,
        haversine(pos.latitude, pos.longitude, pc.port_lat, pc.port_lon) as distance_to_port_km
    from positions pos
    inner join port_cells pc
        on pos.lat_cell = pc.lat_cell
       and pos.lon_cell = pc.lon_cell
    qualify row_number() over (
        partition by pos.vessel_mmsi, pos.event_time
        order by distance_to_port_km
    ) = 1
)

select
    {{ dbt_utils.generate_surrogate_key(['p.vessel_mmsi', 'p.event_time']) }} as telemetry_id,

    -- keys
    p.vessel_mmsi,
    n.nearest_port_id,
    p.event_date,
    p.event_time,

    -- vessel
    p.vessel_category,

    -- movement
    p.latitude,
    p.longitude,
    p.speed_knots,
    p.course_degrees,
    p.navigation_status_desc,
    p.is_sailing,

    -- proximity to port (null when no port within roughly 1 degree)
    n.nearest_port_name,
    round(n.distance_to_port_km, 2)                     as distance_to_port_km,
    coalesce(n.distance_to_port_km <= 25, false)        as is_near_port,

    -- weather
    p.weather_cell_id,
    p.wave_height_m,
    p.wind_speed_kmh,
    p.current_velocity_kmh,
    p.has_weather,
    p.sea_state

from positions p
left join nearest_port n
    on p.vessel_mmsi = n.vessel_mmsi
   and p.event_time = n.event_time
