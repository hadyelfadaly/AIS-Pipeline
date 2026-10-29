-- Does weather slow ships down?
--
-- Sailing pings of cargo/tankers with weather. Each ship is compared with its
-- own median speed in calm water, which removes the "tankers are just slower
-- than container ships" effect. pct_speed_vs_own_calm = -8 means ships sailed
-- 8% slower than their own calm-water speed in that sea state.
-- Absolute averages are kept as well, since in a short data window not every
-- ship meets both calm and rough water.

with sailing as (
    select *
    from {{ ref('int_cargo_positions') }}
    where is_sailing
      and has_weather
),

calm_baseline as (
    select
        vessel_mmsi,
        median(speed_knots) as calm_speed_knots
    from sailing
    where sea_state = 'Calm'
    group by vessel_mmsi
    having count(*) >= 5
),

with_ratio as (
    select
        s.*,
        s.speed_knots / nullif(b.calm_speed_knots, 0) as speed_ratio
    from sailing s
    left join calm_baseline b
        on s.vessel_mmsi = b.vessel_mmsi
)

select
    vessel_category,
    sea_state,
    count(*)                                                    as sailing_pings,
    count(distinct vessel_mmsi)                                 as vessels,
    round(avg(speed_knots), 2)                                  as avg_speed_knots,
    round(avg(wave_height_m), 2)                                as avg_wave_height_m,
    round(avg(wind_speed_kmh), 1)                               as avg_wind_speed_kmh,
    count(speed_ratio)                                          as pings_with_calm_baseline,
    count(distinct iff(speed_ratio is not null, vessel_mmsi, null)) as vessels_with_calm_baseline,
    round((avg(speed_ratio) - 1) * 100, 2)                      as pct_speed_vs_own_calm
from with_ratio
group by vessel_category, sea_state
