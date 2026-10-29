-- One row per AIS static message (a vessel sends these every few minutes).
-- Builds a real ETA timestamp and extracts a port code from the free-text
-- destination the crew types in.
--
-- AIS ETA has month/day/hour/minute but no year, and uses defaults for
-- "not available" (month 0, day 0, hour 24, minute 60). The year is taken as
-- the one that puts the ETA closest to when the message was seen.

with source as (
    select * from {{ source('raw', 'ais_vessels') }}
),

cleaned as (
    select
        user_id                                     as vessel_mmsi,
        trim(name)                                  as vessel_name,
        imo_number,
        trim(call_sign)                             as call_sign,
        ship_type,
        case
            when ship_type between 70 and 79 then 'Cargo'
            when ship_type between 80 and 89 then 'Tanker'
            when ship_type between 60 and 69 then 'Passenger'
            when ship_type between 50 and 59 then 'Special craft'
            when ship_type = 30 then 'Fishing'
            when ship_type between 36 and 37 then 'Pleasure craft'
            else 'Other / unknown'
        end                                         as vessel_category,
        coalesce(ship_type between 70 and 89, false) as is_cargo_or_tanker,
        nullif(draught_m, 0)                        as draught_m,
        seen_at,

        -- destination: upper case, only letters/digits/spaces/'>' kept
        nullif(trim(regexp_replace(destination, '\\s+', ' ')), '')  as destination_raw,
        trim(regexp_replace(
            regexp_replace(upper(coalesce(destination, '')), '[^A-Z0-9> ]', ' '),
            ' +', ' '
        ))                                          as destination_clean,

        -- ETA parts, with AIS "not available" values removed
        iff(eta_month between 1 and 12, eta_month, null) as eta_month,
        iff(eta_day   between 1 and 31, eta_day,   null) as eta_day,
        iff(eta_hour  between 0 and 23, eta_hour,  0)    as eta_hour,
        iff(eta_minute between 0 and 59, eta_minute, 0)  as eta_minute
    from source
),

with_destination as (
    select
        *,
        -- "EGALY>NLRTM" style routes: keep the part after the last '>'
        nullif(trim(split_part(destination_clean, '>', -1)), '') as destination_text
    from cleaned
),

eta_candidates as (
    select
        *,
        iff(regexp_like(replace(destination_text, ' ', ''), '^[A-Z]{5}$'),
            replace(destination_text, ' ', ''), null)            as destination_locode,
        timestamp_ntz_from_parts(year(seen_at) - 1, eta_month, eta_day, eta_hour, eta_minute, 0) as eta_prev_year,
        timestamp_ntz_from_parts(year(seen_at),     eta_month, eta_day, eta_hour, eta_minute, 0) as eta_this_year,
        timestamp_ntz_from_parts(year(seen_at) + 1, eta_month, eta_day, eta_hour, eta_minute, 0) as eta_next_year
    from with_destination
),

with_eta as (
    select
        *,
        case
            -- Feb 31 etc. would roll into the next month: treat as invalid
            when eta_month is null or eta_day is null then null
            when month(eta_this_year) <> eta_month then null
            when abs(datediff('minute', seen_at, eta_prev_year)) < abs(datediff('minute', seen_at, eta_this_year))
                 and abs(datediff('minute', seen_at, eta_prev_year)) < abs(datediff('minute', seen_at, eta_next_year))
                then eta_prev_year
            when abs(datediff('minute', seen_at, eta_next_year)) < abs(datediff('minute', seen_at, eta_this_year))
                then eta_next_year
            else eta_this_year
        end as eta_ts
    from eta_candidates
)

select
    vessel_mmsi,
    vessel_name,
    imo_number,
    call_sign,
    ship_type,
    vessel_category,
    is_cargo_or_tanker,
    draught_m,
    destination_raw,
    destination_text,
    destination_locode,
    eta_ts,
    seen_at
from with_eta
