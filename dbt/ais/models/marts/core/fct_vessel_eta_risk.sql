-- Which ships are at risk of arriving late, and why?
--
-- One row per cargo/tanker vessel that declared a valid ETA. Using its latest
-- position, the distance left to its declared destination and its sailing speed,
-- the model projects the arrival time and compares it with the ETA the crew
-- reported. Sea state at the ship (weather) and how busy the destination port
-- is (PortWatch) are attached as the candidate reasons.
--
-- Caveat: distance is great-circle (straight line), so it underestimates the
-- real sea route. "At risk" is therefore conservative.

with latest_eta as (
    select *
    from {{ ref('stg_ais_vessels') }}
    where is_cargo_or_tanker
      and eta_ts is not null
    qualify row_number() over (partition by vessel_mmsi order by seen_at desc) = 1
),

ports as (
    select * from {{ ref('dim_ports') }}
),

-- PortWatch may list several codes per port ("EGALY;EGEDK"); one row per code
port_codes as (
    select
        p.port_id,
        upper(trim(c.value::string)) as locode
    from ports p,
        lateral split_to_table(
            regexp_replace(
                regexp_replace(upper(coalesce(p.un_locode, '')), '\\s+', ''),  -- "EG ALY" -> "EGALY"
                '[,/|]+', ';'
            ), ';'
        ) c
    where regexp_like(upper(trim(c.value::string)), '^[A-Z]{5}$')
    qualify row_number() over (
        partition by upper(trim(c.value::string))
        order by p.vessel_count_total desc nulls last
    ) = 1
),

-- port names that are unique worldwide, so a name match is unambiguous
port_names as (
    select upper(port_name) as port_name_upper, port_id
    from ports
    qualify count(*) over (partition by upper(port_name)) = 1
),

matched as (
    select
        e.*,
        coalesce(pc.port_id, pn.port_id) as destination_port_id,
        case
            when pc.port_id is not null then 'UN/LOCODE'
            when pn.port_id is not null then 'Port name'
        end as destination_match_method
    from latest_eta e
    left join port_codes pc
        on e.destination_locode = pc.locode
    left join port_names pn
        on e.destination_text = pn.port_name_upper
),

positions as (
    select * from {{ ref('int_cargo_positions') }}
),

latest_position as (
    select
        vessel_mmsi,
        event_time      as position_time,
        latitude,
        longitude,
        speed_knots     as current_speed_knots,
        sea_state       as current_sea_state,
        wave_height_m   as current_wave_height_m,
        wind_speed_kmh  as current_wind_speed_kmh
    from positions
    qualify row_number() over (partition by vessel_mmsi order by event_time desc) = 1
),

voyage_stats as (
    select
        vessel_mmsi,
        avg(iff(is_sailing, speed_knots, null))   as avg_sailing_speed_knots,
        max(wave_height_m)                        as max_wave_height_m,
        avg(wind_speed_kmh)                       as avg_wind_speed_kmh,
        count_if(sea_state = 'Rough') / nullif(count_if(has_weather), 0) as share_rough_sea
    from positions
    group by vessel_mmsi
),

calc as (
    select
        m.vessel_mmsi,
        m.vessel_name,
        m.vessel_category,
        m.destination_raw,
        m.destination_port_id,
        m.destination_match_method,
        m.eta_ts                                       as declared_eta,
        m.seen_at                                      as eta_reported_at,

        lp.position_time,
        lp.latitude,
        lp.longitude,
        lp.current_speed_knots,
        vs.avg_sailing_speed_knots,
        -- speed used for the projection: average while sailing, else current
        coalesce(vs.avg_sailing_speed_knots, lp.current_speed_knots) as reference_speed_knots,

        haversine(lp.latitude, lp.longitude, p.latitude, p.longitude) as distance_to_destination_km,
        datediff('minute', lp.position_time, m.eta_ts) / 60.0       as hours_to_eta,

        lp.current_sea_state,
        lp.current_wave_height_m,
        lp.current_wind_speed_kmh,
        vs.max_wave_height_m,
        vs.avg_wind_speed_kmh,
        vs.share_rough_sea,

        c.congestion_level                             as destination_congestion_level,
        c.congestion_ratio                             as destination_congestion_ratio,
        c.congestion_as_of_date
    from matched m
    inner join latest_position lp
        on m.vessel_mmsi = lp.vessel_mmsi
    left join ports p
        on m.destination_port_id = p.port_id
    left join voyage_stats vs
        on m.vessel_mmsi = vs.vessel_mmsi
    left join {{ ref('int_port_congestion') }} c
        on m.destination_port_id = c.port_id
),

projected as (
    select
        *,
        distance_to_destination_km / 1.852                                   as distance_to_destination_nm,
        iff(hours_to_eta > 0,
            (distance_to_destination_km / 1.852) / hours_to_eta, null)       as required_speed_knots,
        iff(reference_speed_knots >= 1,
            (distance_to_destination_km / 1.852) / reference_speed_knots, null) as hours_needed,
        iff(reference_speed_knots >= 1,
            (distance_to_destination_km / 1.852) / reference_speed_knots - hours_to_eta, null)
                                                                             as projected_delay_hours
    from calc
)

select
    vessel_mmsi,
    vessel_name,
    vessel_category,

    -- destination
    destination_raw,
    destination_port_id,
    destination_match_method,

    -- timing
    declared_eta,
    eta_reported_at,
    position_time,
    round(hours_to_eta, 1)                    as hours_to_eta,
    dateadd('minute', round(hours_needed * 60)::int, position_time) as projected_arrival,
    round(projected_delay_hours, 1)           as projected_delay_hours,

    -- distance and speed
    latitude,
    longitude,
    round(distance_to_destination_km, 1)      as distance_to_destination_km,
    round(distance_to_destination_nm, 1)      as distance_to_destination_nm,
    round(current_speed_knots, 1)             as current_speed_knots,
    round(reference_speed_knots, 1)           as reference_speed_knots,
    round(required_speed_knots, 1)            as required_speed_knots,

    -- the answer
    case
        when destination_port_id is null              then 'Unknown destination'
        when distance_to_destination_km <= 25         then 'At destination'
        when hours_to_eta <= 0                        then 'ETA passed'
        when reference_speed_knots is null
          or reference_speed_knots < 1                then 'Stationary'
        when projected_delay_hours > 6                then 'At risk'
        else 'On track'
    end                                       as eta_status,

    -- candidate reasons: weather at sea
    current_sea_state,
    current_wave_height_m,
    current_wind_speed_kmh,
    max_wave_height_m,
    round(avg_wind_speed_kmh, 1)              as avg_wind_speed_kmh,
    round(share_rough_sea, 3)                 as share_rough_sea,

    -- candidate reasons: destination port congestion
    destination_congestion_level,
    destination_congestion_ratio,
    congestion_as_of_date

from projected
