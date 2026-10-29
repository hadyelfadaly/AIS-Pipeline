-- One row per vessel: its most recent static data.

with messages as (
    select * from {{ ref('stg_ais_vessels') }}
),

seen as (
    select
        vessel_mmsi,
        min(seen_at)  as first_seen_at,
        max(seen_at)  as last_seen_at,
        count(*)      as static_messages
    from messages
    group by vessel_mmsi
),

latest as (
    select *
    from messages
    qualify row_number() over (partition by vessel_mmsi order by seen_at desc) = 1
)

select
    l.vessel_mmsi,
    l.vessel_name,
    l.imo_number,
    l.call_sign,
    l.ship_type,
    l.vessel_category,
    l.is_cargo_or_tanker,
    l.draught_m,
    l.destination_raw      as latest_destination,
    l.eta_ts               as latest_eta,
    s.first_seen_at,
    s.last_seen_at,
    s.static_messages
from latest l
inner join seen s
    on l.vessel_mmsi = s.vessel_mmsi
