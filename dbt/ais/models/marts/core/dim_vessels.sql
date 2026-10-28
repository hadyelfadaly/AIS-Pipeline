{{ config(materialized='table') }}

with telemetry as (
    select * from {{ ref('stg_live_telemetry') }}
),

latest_vessel_state as (
    select
        vessel_mmsi,
        vessel_name,
        ship_type,
        vessel_category,
        destination_reported,
        event_time as last_seen_at,
        row_number() over (partition by vessel_mmsi order by event_time desc) as rn
    from telemetry
)

select
    vessel_mmsi,
    vessel_name,
    ship_type,
    vessel_category,
    destination_reported as latest_destination,
    last_seen_at
from latest_vessel_state
where rn = 1
