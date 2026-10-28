{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw', 'ports_ref') }}
),

cleaned as (
    select
        -- Identifiers
        portid as port_id,
        locode as un_locode,
        trim(portname) as port_name,
        trim(fullname) as full_name,

        -- Geography
        trim(country) as country,
        trim(country_ascii) as country_ascii,
        trim(iso3) as iso3_code,
        trim(continent) as continent,
        lat as latitude,
        lon as longitude,

        -- Fleet & Activity Capacities
        coalesce(vessel_count_total, 0) as vessel_count_total,
        coalesce(vessel_count_container, 0) as vessel_count_container,
        coalesce(vessel_count_dry_bulk, 0) as vessel_count_dry_bulk,
        coalesce(vessel_count_general_cargo, 0) as vessel_count_general_cargo,
        coalesce(vessel_count_roro, 0) as vessel_count_roro,
        coalesce(vessel_count_tanker, 0) as vessel_count_tanker,

        -- Economic Profile
        industry_top1,
        industry_top2,
        industry_top3,
        share_country_maritime_import,
        share_country_maritime_export

    from source
    where portid is not null
)

select * from cleaned
