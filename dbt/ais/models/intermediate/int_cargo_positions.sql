-- Position reports of cargo and tanker vessels only (AIS types 70-89).
-- Silver positions carry no ship type, so the filter comes from the static
-- messages. Every downstream mart starts here, so the expensive filter runs once.
-- Vessels that have not sent a static message yet are left out.

with positions as (
    select * from {{ ref('stg_ais_positions') }}
),

vessels as (
    -- latest static message per vessel, cargo and tankers only
    select vessel_mmsi, ship_type, vessel_category
    from {{ ref('stg_ais_vessels') }}
    qualify row_number() over (partition by vessel_mmsi order by seen_at desc) = 1
        and is_cargo_or_tanker
)
select
    p.*,
    v.ship_type,
    v.vessel_category,
    -- moving under its own power, not anchored, moored or drifting
    (
        coalesce(p.navigational_status, 15) in (0, 8, 15)
        and p.speed_knots >= 3
    ) as is_sailing
from positions p
inner join vessels v
    on p.vessel_mmsi = v.vessel_mmsi
